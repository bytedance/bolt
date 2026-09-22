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

#include <google/protobuf/any.pb.h>
#include <gtest/gtest.h>
#include <lz4.h>
#include <zstd.h>

#include <fmt/format.h>

#include <atomic>
#include <cstdlib>
#include <future>
#include <optional>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/lang/Bits.h>

#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/ColumnSelector.h"
#include "bolt/dwio/common/DirectBufferedInput.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/lance/NativeLanceBitmap.h"
#include "bolt/dwio/lance/NativeLanceBitpack.h"
#include "bolt/dwio/lance/NativeLanceDecoder.h"
#include "bolt/dwio/lance/NativeLanceListOffsets.h"
#include "bolt/dwio/lance/NativeLanceMetadata.h"
#include "bolt/dwio/lance/NativeLancePackedStruct.h"
#include "bolt/dwio/lance/NativeLanceReadPlan.h"
#include "bolt/dwio/lance/NativeLanceReader.h"
#include "bolt/dwio/lance/NativeLanceStructuralDecoder.h"
#include "bolt/dwio/lance/NativeLanceTypeAdapter.h"
#include "bolt/dwio/lance/proto/lance_encodings_v2_0.pb.h"
#include "bolt/dwio/lance/proto/lance_file.pb.h"
#include "bolt/type/HugeInt.h"
#include "bolt/type/Type.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::lance::reader::test {
namespace {

class NativeLanceTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool("native_lance_test");
  }

  struct File {
    std::unique_ptr<dwio::common::BufferedInput> input;
    std::unique_ptr<NativeLanceMetadata> metadata;
  };

  File load(const std::string& fileName) {
    File file;
    file.input = std::make_unique<dwio::common::BufferedInput>(
        std::make_shared<LocalReadFile>("examples/" + fileName), *pool_);
    file.metadata = std::make_unique<NativeLanceMetadata>(*file.input, *pool_);
    return file;
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

std::unique_ptr<dwio::common::BufferedInput> openFile(
    const std::string& fileName,
    memory::MemoryPool& pool) {
  return std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<LocalReadFile>("examples/" + fileName), pool);
}

class CountingReadFile final : public ReadFile {
 public:
  explicit CountingReadFile(std::string contents)
      : delegate_(std::make_shared<InMemoryReadFile>(std::move(contents))) {}

  std::string_view pread(uint64_t offset, uint64_t length, void* buffer)
      const override {
    ++readCalls_;
    bytesRead_ += length;
    return delegate_->pread(offset, length, buffer);
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    ++readCalls_;
    for (const auto& buffer : buffers) {
      bytesRead_ += buffer.size();
    }
    return delegate_->preadv(offset, buffers);
  }

  void preadv(
      folly::Range<const common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs) const override {
    ++readCalls_;
    for (const auto& region : regions) {
      bytesRead_ += region.length;
    }
    delegate_->preadv(regions, iobufs);
  }

  uint64_t size() const override {
    return delegate_->size();
  }

  uint64_t memoryUsage() const override {
    return delegate_->memoryUsage();
  }

  bool shouldCoalesce() const override {
    return true;
  }

  std::string getName() const override {
    return "<CountingReadFile>";
  }

  uint64_t getNaturalReadSize() const override {
    return delegate_->getNaturalReadSize();
  }

  uint64_t readCalls() const {
    return readCalls_;
  }

 private:
  const std::shared_ptr<ReadFile> delegate_;
  mutable std::atomic<uint64_t> readCalls_{0};
};

class TrackingBufferedInput final : public dwio::common::BufferedInput {
 public:
  TrackingBufferedInput(
      std::shared_ptr<ReadFile> readFile,
      memory::MemoryPool& pool)
      : BufferedInput(
            std::move(readFile),
            pool,
            dwio::common::MetricsLog::voidLog(),
            nullptr,
            0) {}

  std::unique_ptr<dwio::common::SeekableInputStream> enqueue(
      common::Region region,
      const dwio::common::StreamIdentifier* streamIdentifier) override {
    enqueuedRegions.push_back(region);
    return BufferedInput::enqueue(region, streamIdentifier);
  }

  void load(dwio::common::LogType logType) override {
    ++loadCalls;
    BufferedInput::load(logType);
  }

  void cancelPendingLoads() override {
    ++cancelCalls;
    BufferedInput::cancelPendingLoads();
  }

  std::vector<common::Region> enqueuedRegions;
  uint64_t loadCalls{0};
  uint64_t cancelCalls{0};
};

std::string decodeBase64(std::string_view encoded) {
  static constexpr std::string_view kAlphabet =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string output;
  output.reserve(encoded.size() * 3 / 4);
  uint32_t accumulator = 0;
  uint32_t bits = 0;
  for (const auto byte : encoded) {
    if (byte == '=') {
      break;
    }
    const auto value = kAlphabet.find(byte);
    BOLT_CHECK_NE(value, std::string_view::npos);
    accumulator = (accumulator << 6) | value;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      output.push_back(static_cast<char>((accumulator >> bits) & 0xff));
    }
  }
  return output;
}

std::string makeBlobResolverFile() {
  return decodeBase64(
      "aW5saW5lSEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhI"
      "SEhISEhISEhISEhISEhISBkAAAAAAAAAAAAAAAAGAAAAAAAAAAAAAAAAAAAAGQAA"
      "AAEBAAAAAAAAAAMAAAAAAAAABwAAAAAAAAAZAAAAAgAAAAAAAAAABAAAAAAAAAAI"
      "AAAAAAAAAC4AAAADAgAAAAAAAAAAAAAAAAAAAAAAAAAVAAAAbWVtb3J5Oi8vZXh0"
      "ZXJuYWwuYmluSEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhI"
      "SEhISEhISEhISEhISEhISAAdOleJSEhISEhISEhISEhISEhISEhISEhISEhISEhI"
      "SEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISEgKqwIKVxIEYmxvYiD/////"
      "//////8BKgZzdHJ1Y3QwAVIbChNsYW5jZS1lbmNvZGluZzpibG9iEgR0cnVlUh0K"
      "FWxhbmNlLWVuY29kaW5nOnBhY2tlZBIEdHJ1ZQolEgRraW5kGP///////////wEg"
      "////////////ASoFdWludDg4AQoqEghwb3NpdGlvbhj///////////8BIP//////"
      "/////wEqBnVpbnQ2NDgBCiYSBHNpemUY////////////ASD///////////8BKgZ1"
      "aW50NjQ4AQopEgdibG9iX2lkGP///////////wEg////////////ASoGdWludDMy"
      "OAEKKhIIYmxvYl91cmkY////////////ASD///////////8BKgZzdHJpbmc4AhAE"
      "CikSJwolCh8vbGFuY2UuZW5jb2RpbmdzLkNvbHVtbkVuY29kaW5nEgIKABJ4CgNA"
      "gAISA4kBBRgEImoSaApmCh0vbGFuY2UuZW5jb2RpbmdzMjEuUGFnZUxheW91dBJF"
      "GkMgICgEMAQ6OGo2CggKBAoCCAgQCAoICgQKAghAEEAKCAoECgIIQBBACggKBAoC"
      "CCAQIAoMCggSBgoECgIIIBggQgEBcAIAAAAAAAClAAAAAAAAAEABAAAAAAAAMAEA"
      "AAAAAABwAgAAAAAAABUDAAAAAAAAJQMAAAAAAAABAAAAAQAAAAIAAgBMQU5D");
}

class TestBlobResolver final : public NativeLanceBlobResolver {
 public:
  std::unique_ptr<dwio::common::BufferedInput> resolve(
      const Request& request,
      memory::MemoryPool& pool) const override {
    std::lock_guard<std::mutex> lock(mutex);
    requests.push_back(request);
    std::string contents;
    switch (request.kind) {
      case Kind::kPacked:
        BOLT_CHECK_EQ(request.blobId, 7);
        contents = "xabc-packed";
        break;
      case Kind::kDedicated:
        BOLT_CHECK_EQ(request.blobId, 8);
        contents = "DEDI";
        break;
      case Kind::kExternal:
        BOLT_CHECK_EQ(request.uri, "memory://external.bin");
        contents = "xxexternal";
        break;
    }
    return std::make_unique<dwio::common::BufferedInput>(
        std::make_shared<InMemoryReadFile>(std::move(contents)), pool);
  }

  mutable std::vector<Request> requests;
  mutable std::mutex mutex;
};

class EnvVarGuard {
 public:
  EnvVarGuard(const char* name, std::optional<std::string> value)
      : name_(name) {
    if (const auto* oldValue = std::getenv(name)) {
      oldValue_ = oldValue;
    }
    if (value.has_value()) {
      ::setenv(name, value->c_str(), 1);
    } else {
      ::unsetenv(name);
    }
  }

  ~EnvVarGuard() {
    if (oldValue_.has_value()) {
      ::setenv(name_.c_str(), oldValue_->c_str(), 1);
    } else {
      ::unsetenv(name_.c_str());
    }
  }

  EnvVarGuard(const EnvVarGuard&) = delete;
  EnvVarGuard& operator=(const EnvVarGuard&) = delete;

 private:
  std::string name_;
  std::optional<std::string> oldValue_;
};

template <typename T>
void appendLittleEndian(std::string& output, T value) {
  value = folly::Endian::little(value);
  output.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

::lance::encodings::ArrayEncoding flatEncoding(
    uint64_t bitsPerValue,
    uint32_t bufferIndex) {
  ::lance::encodings::ArrayEncoding encoding;
  auto* flat = encoding.mutable_flat();
  flat->set_bits_per_value(bitsPerValue);
  flat->mutable_buffer()->set_buffer_index(bufferIndex);
  flat->mutable_buffer()->set_buffer_type(::lance::encodings::Buffer::page);
  return encoding;
}

::lance::file::v2::Encoding directEncoding(
    const ::lance::encodings::ArrayEncoding& encoding) {
  google::protobuf::Any envelope;
  envelope.set_type_url("/lance.encodings.ArrayEncoding");
  envelope.set_value(encoding.SerializeAsString());
  ::lance::file::v2::Encoding result;
  result.mutable_direct()->set_encoding(envelope.SerializeAsString());
  return result;
}

::lance::file::v2::Encoding directColumnEncoding(
    const ::lance::encodings::ColumnEncoding& encoding) {
  google::protobuf::Any envelope;
  envelope.set_type_url("/lance.encodings.ColumnEncoding");
  envelope.set_value(encoding.SerializeAsString());
  ::lance::file::v2::Encoding result;
  result.mutable_direct()->set_encoding(envelope.SerializeAsString());
  return result;
}

std::string makeBooleanTimestampFile(
    std::string_view flagExtensionName = {},
    std::string_view metadataExtensionName = {}) {
  constexpr uint64_t kTimestampOffset = 64;
  std::string data(kTimestampOffset, '\0');
  data[0] = static_cast<char>(0x0d); // Valid rows: 0, 2, 3.
  data[1] = static_cast<char>(0x15); // Values: true, false, true, false, true.
  const std::array<int64_t, 5> timestamps{-1, 0, 1, 1'000'001, 2'000'002};
  for (const auto value : timestamps) {
    appendLittleEndian(data, value);
  }

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(5);
  auto* flag = descriptor.mutable_schema()->add_fields();
  flag->set_name("flag");
  flag->set_parent_id(-1);
  flag->set_logical_type("bool");
  flag->set_nullable(true);
  flag->set_extension_name(
      std::string(flagExtensionName.data(), flagExtensionName.size()));
  if (!metadataExtensionName.empty()) {
    (*flag->mutable_metadata())["ARROW:extension:name"] =
        std::string(metadataExtensionName);
  }
  auto* timestamp = descriptor.mutable_schema()->add_fields();
  timestamp->set_name("ts");
  timestamp->set_id(1);
  timestamp->set_parent_id(-1);
  timestamp->set_logical_type("timestamp:us:-");
  auto* allNull = descriptor.mutable_schema()->add_fields();
  allNull->set_name("all_null");
  allNull->set_id(2);
  allNull->set_parent_id(-1);
  allNull->set_logical_type("int32");
  allNull->set_nullable(true);
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> columnLocations;
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(0);
    page->add_buffer_sizes(1);
    page->add_buffer_offsets(1);
    page->add_buffer_sizes(1);
    page->set_length(5);
    ::lance::encodings::ArrayEncoding encoding;
    auto* nullable = encoding.mutable_nullable()->mutable_some_nulls();
    *nullable->mutable_validity() = flatEncoding(1, 0);
    *nullable->mutable_values() = flatEncoding(1, 1);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    columnLocations.emplace_back(offset, data.size() - offset);
  }
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(kTimestampOffset);
    page->add_buffer_sizes(5 * sizeof(int64_t));
    page->set_length(5);
    ::lance::encodings::ArrayEncoding encoding;
    *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
        flatEncoding(64, 0);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    columnLocations.emplace_back(offset, data.size() - offset);
  }
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->set_length(5);
    ::lance::encodings::ArrayEncoding encoding;
    encoding.mutable_nullable()->mutable_all_nulls();
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    columnLocations.emplace_back(offset, data.size() - offset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto& [offset, length] : columnLocations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(3));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeExtendedPrimitiveFile() {
  constexpr int64_t kMillisPerDay = 86'400'000;
  std::string data;
  std::vector<std::pair<uint64_t, uint64_t>> buffers;
  const auto appendColumn = [&]<typename T>(const std::array<T, 2>& values) {
    const auto offset = data.size();
    for (const auto value : values) {
      appendLittleEndian(data, value);
    }
    buffers.emplace_back(offset, values.size() * sizeof(T));
  };
  appendColumn(
      std::array<uint64_t, 2>{0, std::numeric_limits<uint64_t>::max()});
  appendColumn(std::array<uint16_t, 2>{0x3c00, 0xc000});
  appendColumn(std::array<int64_t, 2>{-kMillisPerDay, 2 * kMillisPerDay});
  appendColumn(std::array<int32_t, 2>{1, 86'399});
  appendColumn(std::array<int32_t, 2>{1, 86'399'999});
  appendColumn(std::array<int64_t, 2>{1, 86'399'999'999});
  appendColumn(std::array<int64_t, 2>{1, 86'399'999'999'999});
  appendColumn(std::array<int64_t, 2>{-2, 3});
  appendColumn(std::array<int64_t, 2>{-2'000'000, 3'000'000});
  appendColumn(std::array<int64_t, 2>{-2'000'000'000, 3'000'000'000});

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(2);
  const std::vector<std::pair<std::string, std::string>> fields{
      {"u64", "uint64"},
      {"f16", "halffloat"},
      {"date64", "date64:ms"},
      {"time_s", "time32:s"},
      {"time_ms", "time32:ms"},
      {"time_us", "time64:us"},
      {"time_ns", "time64:ns"},
      {"duration_s", "duration:s"},
      {"duration_us", "duration:us"},
      {"duration_ns", "duration:ns"}};
  for (uint32_t i = 0; i < fields.size(); ++i) {
    auto* field = descriptor.mutable_schema()->add_fields();
    field->set_name(fields[i].first);
    field->set_id(i);
    field->set_parent_id(-1);
    field->set_logical_type(fields[i].second);
  }
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> columnLocations;
  for (const auto& [offset, length] : buffers) {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(offset);
    page->add_buffer_sizes(length);
    page->set_length(2);
    ::lance::encodings::ArrayEncoding encoding;
    *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
        flatEncoding(length * 4, 0);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto metadataOffset = data.size();
    data.append(column.SerializeAsString());
    columnLocations.emplace_back(metadataOffset, data.size() - metadataOffset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto& [offset, length] : columnLocations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(columnLocations.size()));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string packBits(const std::vector<uint64_t>& values, uint8_t bitWidth) {
  std::string result((values.size() * bitWidth + 7) / 8, '\0');
  const auto mask = (uint64_t{1} << bitWidth) - 1;
  for (size_t row = 0; row < values.size(); ++row) {
    const auto value = values[row] & mask;
    for (uint8_t bit = 0; bit < bitWidth; ++bit) {
      if ((value & (uint64_t{1} << bit)) != 0) {
        const auto outputBit = row * bitWidth + bit;
        result[outputBit / 8] |= static_cast<char>(1U << (outputBit % 8));
      }
    }
  }
  return result;
}

std::vector<uint8_t> packBits(
    const std::vector<uint64_t>& values,
    uint8_t bitWidth,
    uint8_t bitOffset) {
  std::vector<uint8_t> result(
      (bitOffset + values.size() * bitWidth + 7) / 8 + 8, 0);
  const auto mask = bitWidth == 64 ? std::numeric_limits<uint64_t>::max()
                                   : (uint64_t{1} << bitWidth) - 1;
  for (size_t row = 0; row < values.size(); ++row) {
    const auto value = values[row] & mask;
    for (uint8_t bit = 0; bit < bitWidth; ++bit) {
      if ((value & (uint64_t{1} << bit)) != 0) {
        const auto outputBit = bitOffset + row * bitWidth + bit;
        result[outputBit / 8] |= static_cast<uint8_t>(1U << (outputBit % 8));
      }
    }
  }
  return result;
}

std::string makeBitpackedFile() {
  constexpr uint8_t kBitWidth = 5;
  const auto signedData = packBits(
      {static_cast<uint16_t>(-16),
       static_cast<uint16_t>(-3),
       static_cast<uint16_t>(-1),
       0,
       7,
       15},
      kBitWidth);
  const auto unsignedData = packBits({0, 1, 7, 16, 31, 3}, kBitWidth);
  std::string data = signedData + unsignedData;

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(6);
  auto* signedField = descriptor.mutable_schema()->add_fields();
  signedField->set_name("signed_value");
  signedField->set_parent_id(-1);
  signedField->set_logical_type("int16");
  auto* unsignedField = descriptor.mutable_schema()->add_fields();
  unsignedField->set_name("unsigned_value");
  unsignedField->set_id(1);
  unsignedField->set_parent_id(-1);
  unsignedField->set_logical_type("uint16");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> columnLocations;
  for (uint32_t columnIndex = 0; columnIndex < 2; ++columnIndex) {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(
        columnIndex == 0 ? 0 : static_cast<uint64_t>(signedData.size()));
    page->add_buffer_sizes(
        columnIndex == 0 ? signedData.size() : unsignedData.size());
    page->set_length(6);
    ::lance::encodings::ArrayEncoding values;
    auto* bitpacked = values.mutable_bitpacked();
    bitpacked->set_compressed_bits_per_value(kBitWidth);
    bitpacked->set_uncompressed_bits_per_value(16);
    bitpacked->set_signed_(columnIndex == 0);
    bitpacked->mutable_buffer()->set_buffer_index(0);
    bitpacked->mutable_buffer()->set_buffer_type(
        ::lance::encodings::Buffer::page);
    ::lance::encodings::ArrayEncoding encoding;
    *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
        std::move(values);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    columnLocations.emplace_back(offset, data.size() - offset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto& [offset, length] : columnLocations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(2));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string packFastLanesChunks(
    const std::vector<uint64_t>& values,
    uint8_t bitWidth,
    uint8_t uncompressedBits) {
  constexpr uint64_t kChunkRows = 1'024;
  constexpr std::array<uint8_t, 8> kFastLanesOrder{0, 4, 2, 6, 1, 5, 3, 7};
  BOLT_CHECK(
      uncompressedBits == 8 || uncompressedBits == 16 ||
      uncompressedBits == 32 || uncompressedBits == 64);
  BOLT_CHECK_GT(bitWidth, 0);
  BOLT_CHECK_LE(bitWidth, uncompressedBits);
  const auto numChunks = (values.size() + kChunkRows - 1) / kChunkRows;
  const auto chunkBytes = kChunkRows * bitWidth / 8;
  const auto lanes = kChunkRows / uncompressedBits;
  std::string packed(numChunks * chunkBytes, '\0');
  const auto mask = bitWidth == 64 ? std::numeric_limits<uint64_t>::max()
                                   : (uint64_t{1} << bitWidth) - 1;
  for (uint64_t index = 0; index < values.size(); ++index) {
    const auto chunk = index / kChunkRows;
    const auto logical = index % kChunkRows;
    const auto sublane = logical / 128;
    const auto lanePosition = logical % 128;
    uint64_t row = 0;
    uint64_t lane = 0;
    bool found = false;
    for (uint64_t group = 0; group < uncompressedBits / 8; ++group) {
      const auto base = static_cast<uint64_t>(kFastLanesOrder[group]) * 16;
      if (lanePosition >= base && lanePosition < base + lanes) {
        row = group * 8 + sublane;
        lane = lanePosition - base;
        found = true;
        break;
      }
    }
    BOLT_CHECK(found);
    const auto value = values[index] & mask;
    for (uint64_t bit = 0; bit < bitWidth; ++bit) {
      if ((value & (uint64_t{1} << bit)) == 0) {
        continue;
      }
      const auto laneBit = row * bitWidth + bit;
      const auto word = laneBit / uncompressedBits;
      const auto bitInWord = laneBit % uncompressedBits;
      const auto byte = chunk * chunkBytes +
          (word * lanes + lane) * (uncompressedBits / 8) + bitInWord / 8;
      packed[byte] |= static_cast<char>(1U << (bitInWord % 8));
    }
  }
  return packed;
}

std::string makeBitpackedForNonNegFile() {
  constexpr uint64_t kRows = 2'050;
  constexpr std::array<uint8_t, 4> kBitWidths{5, 13, 23, 37};
  constexpr std::array<uint8_t, 4> kOutputWidths{8, 16, 32, 64};
  constexpr std::array<std::string_view, 4> kLogicalTypes{
      "uint8", "uint16", "uint32", "uint64"};
  std::string data;
  std::vector<std::pair<uint64_t, uint64_t>> buffers;
  for (uint32_t column = 0; column < kBitWidths.size(); ++column) {
    const auto mask = (uint64_t{1} << kBitWidths[column]) - 1;
    std::vector<uint64_t> values(kRows);
    for (uint64_t row = 0; row < kRows; ++row) {
      values[row] = (row * 37 + column * 11 + 3) & mask;
    }
    auto packed =
        packFastLanesChunks(values, kBitWidths[column], kOutputWidths[column]);
    buffers.emplace_back(data.size(), packed.size());
    data.append(packed);
  }

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(kRows);
  for (uint32_t column = 0; column < kLogicalTypes.size(); ++column) {
    auto* field = descriptor.mutable_schema()->add_fields();
    field->set_name(fmt::format("u{}", kOutputWidths[column]));
    field->set_id(column);
    field->set_parent_id(-1);
    field->set_logical_type(std::string(kLogicalTypes[column]));
  }
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> columnLocations;
  for (uint32_t column = 0; column < kBitWidths.size(); ++column) {
    ::lance::file::v2::ColumnMetadata metadata;
    auto* page = metadata.add_pages();
    page->add_buffer_offsets(buffers[column].first);
    page->add_buffer_sizes(buffers[column].second);
    page->set_length(kRows);
    ::lance::encodings::ArrayEncoding values;
    auto* bitpacked = values.mutable_bitpacked_for_non_neg();
    bitpacked->set_compressed_bits_per_value(kBitWidths[column]);
    bitpacked->set_uncompressed_bits_per_value(kOutputWidths[column]);
    bitpacked->mutable_buffer()->set_buffer_index(0);
    bitpacked->mutable_buffer()->set_buffer_type(
        ::lance::encodings::Buffer::page);
    ::lance::encodings::ArrayEncoding encoding;
    *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
        std::move(values);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto metadataOffset = data.size();
    data.append(metadata.SerializeAsString());
    columnLocations.emplace_back(metadataOffset, data.size() - metadataOffset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto& [offset, length] : columnLocations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(columnLocations.size()));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeZeroBitpackedV20File() {
  constexpr uint64_t kRows = 2'050;
  constexpr uint64_t kDictionaryValue = 77;
  std::string data;
  std::array<uint64_t, 2> dictionaryItemOffsets{};
  for (auto& offset : dictionaryItemOffsets) {
    offset = data.size();
    appendLittleEndian(data, static_cast<int32_t>(kDictionaryValue));
  }

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(kRows);
  constexpr std::array<std::pair<std::string_view, std::string_view>, 4>
      kFields{{
          {"zero_bitpacked", "uint16"},
          {"zero_fastlanes", "uint32"},
          {"zero_dict_bitpacked", "dict:int32:uint8:false"},
          {"zero_dict_fastlanes", "dict:int32:uint8:false"},
      }};
  for (uint32_t column = 0; column < kFields.size(); ++column) {
    auto* field = descriptor.mutable_schema()->add_fields();
    field->set_name(std::string(kFields[column].first));
    field->set_id(column);
    field->set_parent_id(-1);
    field->set_logical_type(std::string(kFields[column].second));
  }
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto zeroEncoding = [](bool fastLanes, uint64_t uncompressedBits) {
    ::lance::encodings::ArrayEncoding encoding;
    if (fastLanes) {
      auto* bitpacked = encoding.mutable_bitpacked_for_non_neg();
      bitpacked->set_compressed_bits_per_value(0);
      bitpacked->set_uncompressed_bits_per_value(uncompressedBits);
      bitpacked->mutable_buffer()->set_buffer_index(0);
      bitpacked->mutable_buffer()->set_buffer_type(
          ::lance::encodings::Buffer::page);
    } else {
      auto* bitpacked = encoding.mutable_bitpacked();
      bitpacked->set_compressed_bits_per_value(0);
      bitpacked->set_uncompressed_bits_per_value(uncompressedBits);
      bitpacked->set_signed_(false);
      bitpacked->mutable_buffer()->set_buffer_index(0);
      bitpacked->mutable_buffer()->set_buffer_type(
          ::lance::encodings::Buffer::page);
    }
    return encoding;
  };

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> columnLocations;
  for (uint32_t columnIndex = 0; columnIndex < kFields.size(); ++columnIndex) {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(0);
    page->add_buffer_sizes(0);
    page->set_length(kRows);
    const auto isDictionary = columnIndex >= 2;
    auto values = zeroEncoding(
        columnIndex % 2 == 1, isDictionary ? 8 : 16 << columnIndex);
    if (isDictionary) {
      page->add_buffer_offsets(dictionaryItemOffsets[columnIndex - 2]);
      page->add_buffer_sizes(sizeof(int32_t));
      ::lance::encodings::ArrayEncoding dictionaryEncoding;
      *dictionaryEncoding.mutable_dictionary()->mutable_indices() =
          std::move(values);
      *dictionaryEncoding.mutable_dictionary()->mutable_items() =
          flatEncoding(32, 1);
      dictionaryEncoding.mutable_dictionary()->set_num_dictionary_items(1);
      *page->mutable_encoding() = directEncoding(dictionaryEncoding);
    } else {
      ::lance::encodings::ArrayEncoding encoding;
      *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
          std::move(values);
      *page->mutable_encoding() = directEncoding(encoding);
    }
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    columnLocations.emplace_back(offset, data.size() - offset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto& [offset, length] : columnLocations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(columnLocations.size()));
  appendLittleEndian(data, static_cast<uint16_t>(2));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  data.append("LANC");
  return data;
}

std::string makeDictionaryFile() {
  const std::array<uint8_t, 6> indices{1, 2, 0, 1, 3, 2};
  const std::array<uint64_t, 3> itemOffsets{3, 8, 12};
  constexpr std::string_view kItems = "redgreenblue";
  std::string data;
  data.append(reinterpret_cast<const char*>(indices.data()), indices.size());
  const auto itemOffsetsPosition = data.size();
  for (const auto offset : itemOffsets) {
    appendLittleEndian(data, offset);
  }
  const auto itemsPosition = data.size();
  data.append(kItems);

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(indices.size());
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("color");
  field->set_parent_id(-1);
  field->set_logical_type("string");
  field->set_nullable(true);
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(indices.size());
  page->add_buffer_offsets(itemOffsetsPosition);
  page->add_buffer_sizes(itemOffsets.size() * sizeof(uint64_t));
  page->add_buffer_offsets(itemsPosition);
  page->add_buffer_sizes(kItems.size());
  page->set_length(indices.size());

  ::lance::encodings::ArrayEncoding encoding;
  auto* dictionary = encoding.mutable_dictionary();
  *dictionary->mutable_indices() = flatEncoding(8, 0);
  dictionary->set_num_dictionary_items(itemOffsets.size());
  auto* binary = dictionary->mutable_items()->mutable_binary();
  *binary->mutable_indices() = flatEncoding(64, 1);
  *binary->mutable_bytes() = flatEncoding(8, 2);
  binary->set_null_adjustment(kItems.size() + 1);
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeLogicalDictionaryFile() {
  const std::array<uint8_t, 4> indices{2, 0, 1, 2};
  const std::array<int32_t, 3> items{10, 20, 30};
  std::string data;
  data.append(reinterpret_cast<const char*>(indices.data()), indices.size());
  const auto itemsOffset = data.size();
  for (const auto item : items) {
    appendLittleEndian(data, item);
  }

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(indices.size());
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("code");
  field->set_parent_id(-1);
  field->set_logical_type("dict:int32:uint8:false");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(indices.size());
  page->add_buffer_offsets(itemsOffset);
  page->add_buffer_sizes(items.size() * sizeof(int32_t));
  page->set_length(indices.size());
  ::lance::encodings::ArrayEncoding encoding;
  auto* dictionary = encoding.mutable_dictionary();
  *dictionary->mutable_indices() = flatEncoding(8, 0);
  *dictionary->mutable_items()
       ->mutable_nullable()
       ->mutable_no_nulls()
       ->mutable_values() = flatEncoding(32, 1);
  dictionary->set_num_dictionary_items(items.size());
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeFixedSizeBinaryDictionaryFile() {
  const std::array<uint8_t, 4> indices{2, 0, 1, 2};
  constexpr std::string_view kItems = "aaabbbccc";
  std::string data(
      reinterpret_cast<const char*>(indices.data()), indices.size());
  const auto itemsOffset = data.size();
  data.append(kItems);

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(indices.size());
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("code");
  field->set_parent_id(-1);
  field->set_logical_type("dict:fixed_size_binary:3:uint8:false");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(indices.size());
  page->add_buffer_offsets(itemsOffset);
  page->add_buffer_sizes(kItems.size());
  page->set_length(indices.size());
  ::lance::encodings::ArrayEncoding encoding;
  auto* dictionary = encoding.mutable_dictionary();
  *dictionary->mutable_indices() = flatEncoding(8, 0);
  *dictionary->mutable_items()
       ->mutable_nullable()
       ->mutable_no_nulls()
       ->mutable_values() = flatEncoding(24, 1);
  dictionary->set_num_dictionary_items(3);
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeFixedSizeBinaryFile() {
  constexpr std::string_view kValues = "aaabbbcccddd";
  std::string data(kValues);

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(4);
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("value");
  field->set_parent_id(-1);
  field->set_logical_type("string");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(kValues.size());
  page->set_length(4);
  ::lance::encodings::ArrayEncoding values;
  auto* fixed = values.mutable_fixed_size_binary();
  fixed->set_byte_width(3);
  *fixed->mutable_bytes() = flatEncoding(24, 0);
  ::lance::encodings::ArrayEncoding encoding;
  *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
      std::move(values);
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeFlatFixedSizeBinaryFile() {
  constexpr std::string_view kValues = "aaabbbcccddd";
  std::string data(kValues);

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(4);
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("value");
  field->set_parent_id(-1);
  field->set_logical_type("fixed_size_binary:3");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(kValues.size());
  page->set_length(4);
  ::lance::encodings::ArrayEncoding encoding;
  *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
      flatEncoding(24, 0);
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeFixedSizeListFile() {
  std::string data;
  for (int32_t value = 1; value <= 12; ++value) {
    appendLittleEndian(data, value);
  }

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(4);
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("coordinates");
  field->set_parent_id(-1);
  field->set_logical_type("fixed_size_list:int32:3");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(12 * sizeof(int32_t));
  page->set_length(4);
  ::lance::encodings::ArrayEncoding values;
  auto* fixed = values.mutable_fixed_size_list();
  fixed->set_dimension(3);
  *fixed->mutable_items() = flatEncoding(32, 0);
  ::lance::encodings::ArrayEncoding encoding;
  *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
      std::move(values);
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeNestedFixedSizeListFile() {
  std::string data;
  for (int32_t value = 1; value <= 8; ++value) {
    appendLittleEndian(data, value);
  }

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(2);
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("matrix");
  field->set_parent_id(-1);
  field->set_logical_type("fixed_size_list:fixed_size_list:int32:2:2");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(8 * sizeof(int32_t));
  page->set_length(2);
  ::lance::encodings::ArrayEncoding values;
  auto* outer = values.mutable_fixed_size_list();
  outer->set_dimension(2);
  auto* inner = outer->mutable_items()->mutable_fixed_size_list();
  inner->set_dimension(2);
  *inner->mutable_items() = flatEncoding(32, 0);
  ::lance::encodings::ArrayEncoding encoding;
  *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
      std::move(values);
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeFixedSizeBinaryListFile() {
  constexpr std::string_view kValues = "aaabbbcccddd";
  std::string data(kValues);

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(2);
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("pairs");
  field->set_parent_id(-1);
  field->set_logical_type("fixed_size_list:fixed_size_binary:3:2");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(kValues.size());
  page->set_length(2);
  ::lance::encodings::ArrayEncoding values;
  auto* list = values.mutable_fixed_size_list();
  list->set_dimension(2);
  *list->mutable_items() = flatEncoding(24, 0);
  ::lance::encodings::ArrayEncoding encoding;
  *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
      std::move(values);
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeBFloat16ListFile() {
  const std::array<uint16_t, 4> values{0x3f80, 0xc000, 0x4040, 0x0000};
  std::string data;
  for (const auto value : values) {
    appendLittleEndian(data, value);
  }

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(2);
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("values");
  field->set_parent_id(-1);
  field->set_logical_type("fixed_size_list:lance.bfloat16:2");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(values.size() * sizeof(uint16_t));
  page->set_length(2);
  ::lance::encodings::ArrayEncoding valuesEncoding;
  auto* list = valuesEncoding.mutable_fixed_size_list();
  list->set_dimension(2);
  *list->mutable_items() = flatEncoding(16, 0);
  ::lance::encodings::ArrayEncoding encoding;
  *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
      std::move(valuesEncoding);
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string compressValues(std::string_view values, std::string_view scheme) {
  std::string result;
  if (scheme == "zstd_raw") {
    result.resize(ZSTD_compressBound(values.size()));
    auto context = std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)>(
        ZSTD_createCCtx(), ZSTD_freeCCtx);
    BOLT_CHECK_NOT_NULL(context.get());
    BOLT_CHECK(!ZSTD_isError(
        ZSTD_CCtx_setParameter(context.get(), ZSTD_c_contentSizeFlag, 0)));
    const auto compressedSize = ZSTD_compress2(
        context.get(),
        result.data(),
        result.size(),
        values.data(),
        values.size());
    BOLT_CHECK(!ZSTD_isError(compressedSize));
    result.resize(compressedSize);
    BOLT_CHECK_EQ(
        ZSTD_getFrameContentSize(result.data(), result.size()),
        ZSTD_CONTENTSIZE_UNKNOWN);
    return result;
  }
  if (scheme == "zstd") {
    result.resize(sizeof(uint64_t) + ZSTD_compressBound(values.size()));
    auto length = folly::Endian::little(static_cast<uint64_t>(values.size()));
    std::memcpy(result.data(), &length, sizeof(length));
    const auto compressedSize = ZSTD_compress(
        result.data() + sizeof(uint64_t),
        result.size() - sizeof(uint64_t),
        values.data(),
        values.size(),
        0);
    BOLT_CHECK(!ZSTD_isError(compressedSize));
    result.resize(sizeof(uint64_t) + compressedSize);
    return result;
  }
  BOLT_CHECK_EQ(scheme, "lz4");
  result.resize(sizeof(uint32_t) + LZ4_compressBound(values.size()));
  auto length = folly::Endian::little(static_cast<uint32_t>(values.size()));
  std::memcpy(result.data(), &length, sizeof(length));
  const auto compressedSize = LZ4_compress_default(
      values.data(),
      result.data() + sizeof(uint32_t),
      values.size(),
      result.size() - sizeof(uint32_t));
  BOLT_CHECK_GT(compressedSize, 0);
  result.resize(sizeof(uint32_t) + compressedSize);
  return result;
}

std::string makeCompressedFile(std::string_view scheme) {
  std::string values;
  for (int32_t value = 1; value <= 8; ++value) {
    appendLittleEndian(values, value);
  }
  std::string data = compressValues(values, scheme);

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(8);
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("value");
  field->set_parent_id(-1);
  field->set_logical_type("int32");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(schemaOffset);
  page->set_length(8);
  auto valuesEncoding = flatEncoding(32, 0);
  valuesEncoding.mutable_flat()->mutable_compression()->set_scheme(
      scheme == "zstd_raw" ? "zstd" : std::string(scheme));
  ::lance::encodings::ArrayEncoding encoding;
  *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
      std::move(valuesEncoding);
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeManyCompressedColumnsFile(uint32_t numColumns) {
  constexpr uint64_t kRows = 8;
  std::string data;
  std::vector<std::pair<uint64_t, uint64_t>> valueLocations;
  valueLocations.reserve(numColumns);
  for (uint32_t column = 0; column < numColumns; ++column) {
    std::string values;
    for (uint64_t row = 0; row < kRows; ++row) {
      appendLittleEndian(values, static_cast<int32_t>(column * 100 + row));
    }
    auto compressed = compressValues(values, "zstd");
    valueLocations.emplace_back(data.size(), compressed.size());
    data.append(compressed);
  }

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(kRows);
  for (uint32_t column = 0; column < numColumns; ++column) {
    auto* field = descriptor.mutable_schema()->add_fields();
    field->set_name(fmt::format("c{}", column));
    field->set_id(column);
    field->set_parent_id(-1);
    field->set_logical_type("int32");
  }
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> columnLocations;
  columnLocations.reserve(numColumns);
  for (const auto [offset, length] : valueLocations) {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(offset);
    page->add_buffer_sizes(length);
    page->set_length(kRows);
    auto valuesEncoding = flatEncoding(32, 0);
    valuesEncoding.mutable_flat()->mutable_compression()->set_scheme("zstd");
    ::lance::encodings::ArrayEncoding encoding;
    *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
        std::move(valuesEncoding);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto metadataOffset = data.size();
    data.append(column.SerializeAsString());
    columnLocations.emplace_back(metadataOffset, data.size() - metadataOffset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto [offset, length] : columnLocations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, numColumns);
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeCompressedListFile() {
  const std::array<uint64_t, 4> offsets{2, 2, 5, 6};
  std::string rawOffsets;
  for (const auto offset : offsets) {
    appendLittleEndian(rawOffsets, offset);
  }
  const auto compressedOffsets = compressValues(rawOffsets, "zstd");
  const std::array<int32_t, 6> items{10, 20, 30, 40, 50, 60};
  std::string data = compressedOffsets;
  const auto itemsOffset = data.size();
  const auto compressedItems = compressValues(
      std::string_view(
          reinterpret_cast<const char*>(items.data()),
          items.size() * sizeof(int32_t)),
      "zstd");
  data.append(compressedItems);

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(offsets.size());
  auto* list = descriptor.mutable_schema()->add_fields();
  list->set_name("values");
  list->set_id(0);
  list->set_parent_id(-1);
  list->set_logical_type("list");
  auto* item = descriptor.mutable_schema()->add_fields();
  item->set_name("item");
  item->set_id(1);
  item->set_parent_id(0);
  item->set_logical_type("int32");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> locations;
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(0);
    page->add_buffer_sizes(compressedOffsets.size());
    page->set_length(offsets.size());
    ::lance::encodings::ArrayEncoding encoding;
    auto encodedOffsets = flatEncoding(64, 0);
    encodedOffsets.mutable_flat()->mutable_compression()->set_scheme("zstd");
    *encoding.mutable_list()->mutable_offsets() = std::move(encodedOffsets);
    encoding.mutable_list()->set_null_offset_adjustment(items.size() + 1);
    encoding.mutable_list()->set_num_items(items.size());
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    locations.emplace_back(offset, data.size() - offset);
  }
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(itemsOffset);
    page->add_buffer_sizes(compressedItems.size());
    page->set_length(items.size());
    ::lance::encodings::ArrayEncoding encoding;
    auto encodedItems = flatEncoding(32, 0);
    encodedItems.mutable_flat()->mutable_compression()->set_scheme("zstd");
    *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
        std::move(encodedItems);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    locations.emplace_back(offset, data.size() - offset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto& [offset, length] : locations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(locations.size()));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeBitpackedListOffsetsFile() {
  constexpr uint64_t kRows = 2'050;
  constexpr uint8_t kBitWidth = 13;
  std::vector<uint64_t> offsets;
  std::vector<int32_t> items;
  offsets.reserve(kRows);
  for (uint64_t row = 0; row < kRows; ++row) {
    const auto size = row % 4;
    for (uint64_t item = 0; item < size; ++item) {
      items.push_back(static_cast<int32_t>(row * 10 + item));
    }
    offsets.push_back(items.size());
  }
  auto data = packFastLanesChunks(offsets, kBitWidth, 64);
  const auto itemsOffset = data.size();
  for (const auto item : items) {
    appendLittleEndian(data, item);
  }

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(kRows);
  auto* list = descriptor.mutable_schema()->add_fields();
  list->set_name("values");
  list->set_id(0);
  list->set_parent_id(-1);
  list->set_logical_type("list");
  auto* item = descriptor.mutable_schema()->add_fields();
  item->set_name("item");
  item->set_id(1);
  item->set_parent_id(0);
  item->set_logical_type("int32");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> locations;
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(0);
    page->add_buffer_sizes(itemsOffset);
    page->set_length(kRows);
    ::lance::encodings::ArrayEncoding encoding;
    auto* encodedOffsets = encoding.mutable_list()
                               ->mutable_offsets()
                               ->mutable_bitpacked_for_non_neg();
    encodedOffsets->set_compressed_bits_per_value(kBitWidth);
    encodedOffsets->set_uncompressed_bits_per_value(64);
    encodedOffsets->mutable_buffer()->set_buffer_index(0);
    encodedOffsets->mutable_buffer()->set_buffer_type(
        ::lance::encodings::Buffer::page);
    encoding.mutable_list()->set_null_offset_adjustment(items.size() + 1);
    encoding.mutable_list()->set_num_items(items.size());
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    locations.emplace_back(offset, data.size() - offset);
  }
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(itemsOffset);
    page->add_buffer_sizes(items.size() * sizeof(int32_t));
    page->set_length(items.size());
    ::lance::encodings::ArrayEncoding encoding;
    *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
        flatEncoding(32, 0);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    locations.emplace_back(offset, data.size() - offset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto [offset, length] : locations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(locations.size()));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeBitpackedLegacyBinaryOffsetsFile() {
  constexpr uint64_t kRows = 2'050;
  constexpr uint8_t kBitWidth = 12;
  std::vector<uint64_t> offsets;
  std::string values;
  offsets.reserve(kRows);
  for (uint64_t row = 0; row < kRows; ++row) {
    for (uint64_t item = 0; item < row % 4; ++item) {
      values.push_back(static_cast<char>('a' + (row + item) % 26));
    }
    offsets.push_back(values.size());
  }
  auto data = packFastLanesChunks(offsets, kBitWidth, 64);
  const auto valuesOffset = data.size();
  data.append(values);

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(kRows);
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("value");
  field->set_id(0);
  field->set_parent_id(-1);
  field->set_logical_type("string");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> locations;
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(0);
    page->add_buffer_sizes(valuesOffset);
    page->set_length(kRows);
    ::lance::encodings::ArrayEncoding encoding;
    auto* encodedOffsets = encoding.mutable_list()
                               ->mutable_offsets()
                               ->mutable_bitpacked_for_non_neg();
    encodedOffsets->set_compressed_bits_per_value(kBitWidth);
    encodedOffsets->set_uncompressed_bits_per_value(64);
    encodedOffsets->mutable_buffer()->set_buffer_index(0);
    encodedOffsets->mutable_buffer()->set_buffer_type(
        ::lance::encodings::Buffer::page);
    encoding.mutable_list()->set_null_offset_adjustment(values.size() + 1);
    encoding.mutable_list()->set_num_items(values.size());
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    locations.emplace_back(offset, data.size() - offset);
  }
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(valuesOffset);
    page->add_buffer_sizes(values.size());
    page->set_length(values.size());
    ::lance::encodings::ArrayEncoding encoding;
    *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
        flatEncoding(8, 0);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    locations.emplace_back(offset, data.size() - offset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto [offset, length] : locations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(locations.size()));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makePackedStructFile() {
  std::string data;
  for (int32_t row = 0; row < 4; ++row) {
    appendLittleEndian(data, static_cast<int64_t>(100 + row));
    appendLittleEndian(data, static_cast<int32_t>(200 + row));
    data.push_back(static_cast<char>(10 + row));
  }

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(4);
  auto* parent = descriptor.mutable_schema()->add_fields();
  parent->set_name("packed");
  parent->set_id(0);
  parent->set_parent_id(-1);
  parent->set_logical_type("struct");
  (*parent->mutable_metadata())["packed"] = "true";
  for (const auto& [name, id, logicalType] :
       std::vector<std::tuple<std::string, int32_t, std::string>>{
           {"x", 1, "int64"}, {"y", 2, "int32"}, {"z", 3, "uint8"}}) {
    auto* child = descriptor.mutable_schema()->add_fields();
    child->set_name(name);
    child->set_id(id);
    child->set_parent_id(0);
    child->set_logical_type(logicalType);
  }
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(4 * (sizeof(int64_t) + sizeof(int32_t) + 1));
  page->set_length(4);
  ::lance::encodings::ArrayEncoding encoding;
  auto* packed = encoding.mutable_packed_struct();
  *packed->add_inner() = flatEncoding(64, 0);
  *packed->add_inner() = flatEncoding(32, 0);
  *packed->add_inner() = flatEncoding(8, 0);
  packed->mutable_buffer()->set_buffer_index(0);
  packed->mutable_buffer()->set_buffer_type(::lance::encodings::Buffer::page);
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makePackedNestedFixedSizeListFile() {
  std::string data;
  for (int16_t value = 1; value <= 8; ++value) {
    appendLittleEndian(data, value);
  }

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(2);
  auto* parent = descriptor.mutable_schema()->add_fields();
  parent->set_name("packed");
  parent->set_id(0);
  parent->set_parent_id(-1);
  parent->set_logical_type("struct");
  (*parent->mutable_metadata())["lance-encoding:packed"] = "true";
  auto* child = descriptor.mutable_schema()->add_fields();
  child->set_name("matrix");
  child->set_id(1);
  child->set_parent_id(0);
  child->set_logical_type("fixed_size_list:fixed_size_list:int16:2:2");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(8 * sizeof(int16_t));
  page->set_length(2);
  ::lance::encodings::ArrayEncoding encoding;
  auto* packed = encoding.mutable_packed_struct();
  auto* outer = packed->add_inner()->mutable_fixed_size_list();
  outer->set_dimension(2);
  auto* inner = outer->mutable_items()->mutable_fixed_size_list();
  inner->set_dimension(2);
  *inner->mutable_items() = flatEncoding(16, 0);
  packed->mutable_buffer()->set_buffer_index(0);
  packed->mutable_buffer()->set_buffer_type(::lance::encodings::Buffer::page);
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeFsstFile() {
  // Codes 0 and 1 expand to "hello" and "world". Escape (255) copies
  // the following byte verbatim.
  const std::array<uint8_t, 5> compressed{0, 1, 255, '!', 0};
  const std::array<uint64_t, 4> offsets{1, 2, 4, 5};
  std::string data;
  for (const auto offset : offsets) {
    appendLittleEndian(data, offset);
  }
  const auto bytesOffset = data.size();
  data.append(
      reinterpret_cast<const char*>(compressed.data()), compressed.size());

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(4);
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("value");
  field->set_parent_id(-1);
  field->set_logical_type("string");
  field->set_nullable(true);
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  auto* page = column.add_pages();
  page->add_buffer_offsets(0);
  page->add_buffer_sizes(offsets.size() * sizeof(uint64_t));
  page->add_buffer_offsets(bytesOffset);
  page->add_buffer_sizes(compressed.size());
  page->set_length(4);
  ::lance::encodings::ArrayEncoding encoding;
  auto* fsst = encoding.mutable_fsst();
  auto* binary = fsst->mutable_binary()->mutable_binary();
  *binary->mutable_indices() = flatEncoding(64, 0);
  *binary->mutable_bytes() = flatEncoding(8, 1);
  binary->set_null_adjustment(compressed.size() + 1);
  std::string symbolTable(8 + 256 * 8 + 256, '\0');
  const uint64_t header =
      (uint64_t{0x46535354} << 32) | (uint64_t{1} << 24) | 2;
  auto littleHeader = folly::Endian::little(header);
  std::memcpy(symbolTable.data(), &littleHeader, sizeof(littleHeader));
  std::memcpy(symbolTable.data() + 8, "hello", 5);
  std::memcpy(symbolTable.data() + 16, "world", 5);
  symbolTable[8 + 2 * 8] = 5;
  symbolTable[8 + 2 * 8 + 1] = 5;
  fsst->set_symbol_table(std::move(symbolTable));
  *page->mutable_encoding() = directEncoding(encoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeMultiPageListFile() {
  constexpr uint64_t kSecondOffsets = 64;
  constexpr uint64_t kFirstItems = 128;
  constexpr uint64_t kSecondItems = 192;
  std::string data(kSecondItems, '\0');
  const std::array<uint64_t, 2> firstOffsets{2, 3};
  const std::array<uint64_t, 3> secondOffsets{3, 3, 5};
  std::memcpy(
      data.data(), firstOffsets.data(), firstOffsets.size() * sizeof(uint64_t));
  std::memcpy(
      data.data() + kSecondOffsets,
      secondOffsets.data(),
      secondOffsets.size() * sizeof(uint64_t));
  const std::array<int32_t, 3> firstItems{1, 2, 3};
  const std::array<int32_t, 5> secondItems{4, 5, 6, 7, 8};
  std::memcpy(
      data.data() + kFirstItems,
      firstItems.data(),
      firstItems.size() * sizeof(int32_t));
  data.resize(kSecondItems + secondItems.size() * sizeof(int32_t));
  std::memcpy(
      data.data() + kSecondItems,
      secondItems.data(),
      secondItems.size() * sizeof(int32_t));

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(5);
  auto* list = descriptor.mutable_schema()->add_fields();
  list->set_name("values");
  list->set_id(0);
  list->set_parent_id(-1);
  list->set_logical_type("list");
  auto* item = descriptor.mutable_schema()->add_fields();
  item->set_name("item");
  item->set_id(1);
  item->set_parent_id(0);
  item->set_logical_type("int32");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> locations;
  {
    ::lance::file::v2::ColumnMetadata column;
    for (const auto& [offset, rows, items] :
         std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>{
             {0, 2, 3}, {kSecondOffsets, 3, 5}}) {
      auto* page = column.add_pages();
      page->add_buffer_offsets(offset);
      page->add_buffer_sizes(rows * sizeof(uint64_t));
      page->set_length(rows);
      ::lance::encodings::ArrayEncoding encoding;
      *encoding.mutable_list()->mutable_offsets() = flatEncoding(64, 0);
      encoding.mutable_list()->set_null_offset_adjustment(items + 1);
      encoding.mutable_list()->set_num_items(items);
      *page->mutable_encoding() = directEncoding(encoding);
    }
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    locations.emplace_back(offset, data.size() - offset);
  }
  {
    ::lance::file::v2::ColumnMetadata column;
    for (const auto& [offset, rows] :
         std::vector<std::pair<uint64_t, uint64_t>>{
             {kFirstItems, 3}, {kSecondItems, 5}}) {
      auto* page = column.add_pages();
      page->add_buffer_offsets(offset);
      page->add_buffer_sizes(rows * sizeof(int32_t));
      page->set_length(rows);
      ::lance::encodings::ArrayEncoding encoding;
      *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
          flatEncoding(32, 0);
      *page->mutable_encoding() = directEncoding(encoding);
    }
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    locations.emplace_back(offset, data.size() - offset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto& [offset, length] : locations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(2));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeNestedListFile() {
  constexpr uint64_t kInnerOffsets = 64;
  constexpr uint64_t kValues = 128;
  // Outer rows: [[1, 2], [], [[3, 4], [5]], [[6]]].
  const std::array<uint64_t, 4> outerOffsets{2, 2, 4, 5};
  const std::array<uint64_t, 5> innerOffsets{2, 2, 4, 5, 6};
  const std::array<int32_t, 6> values{1, 2, 3, 4, 5, 6};
  std::string data(kValues, '\0');
  std::memcpy(
      data.data(), outerOffsets.data(), outerOffsets.size() * sizeof(uint64_t));
  std::memcpy(
      data.data() + kInnerOffsets,
      innerOffsets.data(),
      innerOffsets.size() * sizeof(uint64_t));
  data.resize(kValues + values.size() * sizeof(int32_t));
  std::memcpy(
      data.data() + kValues, values.data(), values.size() * sizeof(int32_t));

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(outerOffsets.size());
  for (const auto& [name, id, parentId, logicalType] :
       std::vector<std::tuple<std::string, int32_t, int32_t, std::string>>{
           {"outer", 0, -1, "list"},
           {"inner", 1, 0, "list"},
           {"item", 2, 1, "int32"}}) {
    auto* field = descriptor.mutable_schema()->add_fields();
    field->set_name(name);
    field->set_id(id);
    field->set_parent_id(parentId);
    field->set_logical_type(logicalType);
  }
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> locations;
  for (const auto& [offset, rowCount, itemCount] :
       std::array<std::tuple<uint64_t, uint64_t, uint64_t>, 2>{
           std::tuple<uint64_t, uint64_t, uint64_t>{
               0, outerOffsets.size(), innerOffsets.size()},
           std::tuple<uint64_t, uint64_t, uint64_t>{
               kInnerOffsets, innerOffsets.size(), values.size()}}) {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(offset);
    page->add_buffer_sizes(rowCount * sizeof(uint64_t));
    page->set_length(rowCount);
    ::lance::encodings::ArrayEncoding encoding;
    *encoding.mutable_list()->mutable_offsets() = flatEncoding(64, 0);
    encoding.mutable_list()->set_null_offset_adjustment(itemCount + 1);
    encoding.mutable_list()->set_num_items(itemCount);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto metadataOffset = data.size();
    data.append(column.SerializeAsString());
    locations.emplace_back(metadataOffset, data.size() - metadataOffset);
  }
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(kValues);
    page->add_buffer_sizes(values.size() * sizeof(int32_t));
    page->set_length(values.size());
    ::lance::encodings::ArrayEncoding encoding;
    *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
        flatEncoding(32, 0);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto metadataOffset = data.size();
    data.append(column.SerializeAsString());
    locations.emplace_back(metadataOffset, data.size() - metadataOffset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto& [offset, length] : locations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(locations.size()));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeMapFile() {
  constexpr uint64_t kKeysOffset = 64;
  constexpr uint64_t kValuesOffset = 128;
  constexpr uint64_t kNumItems = 5;
  constexpr uint64_t kNullOffsetAdjustment = kNumItems + 1;
  const std::array<uint64_t, 5> offsets{2, kNullOffsetAdjustment + 2, 2, 3, 5};
  const std::array<int32_t, kNumItems> keys{1, 2, 3, 4, 5};
  const std::array<int64_t, kNumItems> values{10, 20, 30, 40, 50};
  std::string data(kValuesOffset, '\0');
  std::memcpy(data.data(), offsets.data(), offsets.size() * sizeof(uint64_t));
  std::memcpy(
      data.data() + kKeysOffset, keys.data(), keys.size() * sizeof(int32_t));
  data.resize(kValuesOffset + values.size() * sizeof(int64_t));
  std::memcpy(
      data.data() + kValuesOffset,
      values.data(),
      values.size() * sizeof(int64_t));

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(offsets.size());
  for (const auto& [name, id, parentId, logicalType, nullable] : std::vector<
           std::tuple<std::string, int32_t, int32_t, std::string, bool>>{
           {"attributes", 0, -1, "map", true},
           {"entries", 1, 0, "struct", false},
           {"key", 2, 1, "int32", false},
           {"value", 3, 1, "int64", false}}) {
    auto* field = descriptor.mutable_schema()->add_fields();
    field->set_name(name);
    field->set_id(id);
    field->set_parent_id(parentId);
    field->set_logical_type(logicalType);
    field->set_nullable(nullable);
  }
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> columnLocations;
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(0);
    page->add_buffer_sizes(offsets.size() * sizeof(uint64_t));
    page->set_length(offsets.size());
    ::lance::encodings::ArrayEncoding encoding;
    *encoding.mutable_list()->mutable_offsets() = flatEncoding(64, 0);
    encoding.mutable_list()->set_null_offset_adjustment(kNullOffsetAdjustment);
    encoding.mutable_list()->set_num_items(kNumItems);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    columnLocations.emplace_back(offset, data.size() - offset);
  }
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->set_length(kNumItems);
    ::lance::encodings::ArrayEncoding encoding;
    encoding.mutable_struct_();
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    columnLocations.emplace_back(offset, data.size() - offset);
  }
  for (const auto& [offset, bitsPerValue] :
       std::vector<std::pair<uint64_t, uint64_t>>{
           {kKeysOffset, 32}, {kValuesOffset, 64}}) {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->add_buffer_offsets(offset);
    page->add_buffer_sizes(kNumItems * bitsPerValue / 8);
    page->set_length(kNumItems);
    ::lance::encodings::ArrayEncoding encoding;
    *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
        flatEncoding(bitsPerValue, 0);
    *page->mutable_encoding() = directEncoding(encoding);
    const auto metadataOffset = data.size();
    data.append(column.SerializeAsString());
    columnLocations.emplace_back(metadataOffset, data.size() - metadataOffset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto& [offset, length] : columnLocations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(columnLocations.size()));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeBlobFile() {
  constexpr uint64_t kFirstPayloadOffset = 64;
  constexpr uint64_t kSecondPayloadOffset = 80;
  constexpr uint64_t kFirstDescriptionsOffset = 128;
  constexpr uint64_t kSecondDescriptionsOffset = 192;
  std::string data(kFirstPayloadOffset, '\0');
  data.append("alpha", 5);
  data.resize(kSecondPayloadOffset, '\0');
  data.append("xyz", 3);
  data.resize(kFirstDescriptionsOffset, '\0');
  for (const auto& [position, size] :
       std::array<std::pair<uint64_t, uint64_t>, 2>{
           std::pair<uint64_t, uint64_t>{kFirstPayloadOffset, 5},
           std::pair<uint64_t, uint64_t>{1, 0}}) {
    appendLittleEndian(data, position);
    appendLittleEndian(data, size);
  }
  data.resize(kSecondDescriptionsOffset, '\0');
  for (const auto& [position, size] :
       std::array<std::pair<uint64_t, uint64_t>, 2>{
           std::pair<uint64_t, uint64_t>{0, 0},
           std::pair<uint64_t, uint64_t>{kSecondPayloadOffset, 3}}) {
    appendLittleEndian(data, position);
    appendLittleEndian(data, size);
  }

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(4);
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("payload");
  field->set_id(0);
  field->set_parent_id(-1);
  field->set_logical_type("large_binary");
  field->set_nullable(true);
  (*field->mutable_metadata())["lance-encoding:blob"] = "true";
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  for (const auto descriptionsOffset :
       {kFirstDescriptionsOffset, kSecondDescriptionsOffset}) {
    auto* page = column.add_pages();
    page->add_buffer_offsets(descriptionsOffset);
    page->add_buffer_sizes(2 * 2 * sizeof(uint64_t));
    page->set_length(2);
    ::lance::encodings::ArrayEncoding encoding;
    auto* packed = encoding.mutable_packed_struct();
    *packed->add_inner() = flatEncoding(64, 0);
    *packed->add_inner() = flatEncoding(64, 0);
    packed->mutable_buffer()->set_buffer_index(0);
    packed->mutable_buffer()->set_buffer_type(::lance::encodings::Buffer::page);
    *page->mutable_encoding() = directEncoding(encoding);
  }
  ::lance::encodings::ColumnEncoding columnEncoding;
  columnEncoding.mutable_blob()->mutable_inner()->mutable_values();
  *column.mutable_encoding() = directColumnEncoding(columnEncoding);
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;

  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeStructSplitAnchorFile() {
  constexpr uint64_t kSecondPageOffset = 64;
  std::string data(kSecondPageOffset, '\0');
  const std::array<int32_t, 2> first{1, 2};
  const std::array<int32_t, 2> second{3, 4};
  std::memcpy(data.data(), first.data(), first.size() * sizeof(int32_t));
  data.resize(kSecondPageOffset + second.size() * sizeof(int32_t));
  std::memcpy(
      data.data() + kSecondPageOffset,
      second.data(),
      second.size() * sizeof(int32_t));

  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(4);
  auto* parent = descriptor.mutable_schema()->add_fields();
  parent->set_name("record");
  parent->set_id(0);
  parent->set_parent_id(-1);
  parent->set_logical_type("struct");
  auto* child = descriptor.mutable_schema()->add_fields();
  child->set_name("value");
  child->set_id(1);
  child->set_parent_id(0);
  child->set_logical_type("int32");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  std::vector<std::pair<uint64_t, uint64_t>> locations;
  {
    ::lance::file::v2::ColumnMetadata column;
    auto* page = column.add_pages();
    page->set_length(4);
    ::lance::encodings::ArrayEncoding encoding;
    encoding.mutable_struct_();
    *page->mutable_encoding() = directEncoding(encoding);
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    locations.emplace_back(offset, data.size() - offset);
  }
  {
    ::lance::file::v2::ColumnMetadata column;
    for (const auto offset : {uint64_t{0}, kSecondPageOffset}) {
      auto* page = column.add_pages();
      page->add_buffer_offsets(offset);
      page->add_buffer_sizes(2 * sizeof(int32_t));
      page->set_length(2);
      ::lance::encodings::ArrayEncoding encoding;
      *encoding.mutable_nullable()->mutable_no_nulls()->mutable_values() =
          flatEncoding(32, 0);
      *page->mutable_encoding() = directEncoding(encoding);
    }
    const auto offset = data.size();
    data.append(column.SerializeAsString());
    locations.emplace_back(offset, data.size() - offset);
  }

  const auto columnMetadataOffsetsStart = data.size();
  for (const auto& [offset, length] : locations) {
    appendLittleEndian(data, offset);
    appendLittleEndian(data, length);
  }
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(locations.size()));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeEmptyPackedStructFile() {
  std::string data;
  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(0);
  auto* parent = descriptor.mutable_schema()->add_fields();
  parent->set_name("record");
  parent->set_id(0);
  parent->set_parent_id(-1);
  parent->set_logical_type("struct");
  (*parent->mutable_metadata())["lance-encoding:packed"] = "true";
  auto* child = descriptor.mutable_schema()->add_fields();
  child->set_name("value");
  child->set_id(1);
  child->set_parent_id(0);
  child->set_logical_type("int32");
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;

  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;
  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(0));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

std::string makeUnsupportedLogicalTypeFile(std::string_view logicalType) {
  std::string data;
  const auto schemaOffset = data.size();
  ::lance::file::FileDescriptor descriptor;
  descriptor.set_length(0);
  auto* field = descriptor.mutable_schema()->add_fields();
  field->set_name("value");
  field->set_id(0);
  field->set_parent_id(-1);
  field->set_logical_type(std::string(logicalType.data(), logicalType.size()));
  data.append(descriptor.SerializeAsString());
  const auto schemaLength = data.size() - schemaOffset;
  const auto columnMetadataStart = data.size();
  ::lance::file::v2::ColumnMetadata column;
  const auto columnMetadataOffset = data.size();
  data.append(column.SerializeAsString());
  const auto columnMetadataLength = data.size() - columnMetadataOffset;
  const auto columnMetadataOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffset));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataLength));
  const auto globalBufferOffsetsStart = data.size();
  appendLittleEndian(data, static_cast<uint64_t>(schemaOffset));
  appendLittleEndian(data, static_cast<uint64_t>(schemaLength));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataStart));
  appendLittleEndian(data, static_cast<uint64_t>(columnMetadataOffsetsStart));
  appendLittleEndian(data, static_cast<uint64_t>(globalBufferOffsetsStart));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint32_t>(1));
  appendLittleEndian(data, static_cast<uint16_t>(2));
  appendLittleEndian(data, static_cast<uint16_t>(3));
  data.append("LANC");
  return data;
}

TEST_F(NativeLanceTest, sampleMetadata) {
  const auto file = load("sample.lance");
  const auto& metadata = file.metadata;

  EXPECT_EQ(metadata->footer().majorVersion, 0);
  EXPECT_EQ(metadata->footer().minorVersion, 3);
  EXPECT_EQ(metadata->numRows(), 20);
  EXPECT_EQ(metadata->rowType()->toString(), "ROW<a:BIGINT,b:DOUBLE>");
  ASSERT_EQ(metadata->globalBuffers().size(), 1);
  EXPECT_EQ(metadata->globalBuffers()[0].offset, 384);
  EXPECT_EQ(metadata->globalBuffers()[0].length, 79);

  ASSERT_EQ(metadata->columns().size(), 2);
  ASSERT_EQ(metadata->columns()[0].pages_size(), 1);
  EXPECT_EQ(metadata->columns()[0].pages(0).buffer_offsets(0), 0);
  EXPECT_EQ(metadata->columns()[0].pages(0).buffer_sizes(0), 160);
  EXPECT_EQ(metadata->columns()[0].pages(0).length(), 20);
  EXPECT_EQ(metadata->columns()[1].pages(0).buffer_offsets(0), 192);
  EXPECT_EQ(metadata->columns()[1].pages(0).buffer_sizes(0), 160);
}

TEST_F(NativeLanceTest, rejectsUnknownArrowExtension) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(
          makeBooleanTimestampFile("example.semantic.boolean")),
      *pool_);
  EXPECT_THROW(
      {
        try {
          NativeLanceMetadata metadata(*input, *pool_);
        } catch (const BoltException& error) {
          EXPECT_NE(
              error.message().find("Unsupported Lance Arrow extension"),
              std::string::npos);
          EXPECT_NE(
              error.message().find("example.semantic.boolean"),
              std::string::npos);
          throw;
        }
      },
      BoltException);

  input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(
          makeBooleanTimestampFile({}, "example.metadata.boolean")),
      *pool_);
  EXPECT_THROW(
      { NativeLanceMetadata metadata(*input, *pool_); }, BoltException);

  input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(
          makeBooleanTimestampFile("example.first", "example.second")),
      *pool_);
  EXPECT_THROW(
      { NativeLanceMetadata metadata(*input, *pool_); }, BoltException);
}

TEST_F(NativeLanceTest, preservesBuiltInSemanticTypes) {
  const auto file = load("type_matrix_v2_2.lance");
  const auto type = file.metadata->rowType();
  ASSERT_EQ(type->size(), 8);
  EXPECT_EQ(nativeLanceSemanticType(type->childAt(0)), "time32:ms");
  EXPECT_EQ(nativeLanceSemanticType(type->childAt(1)), "time64:us");
  EXPECT_FALSE(nativeLanceSemanticType(type->childAt(3)).has_value());
  EXPECT_EQ(nativeLanceSemanticType(type->childAt(4)), "timestamp:ms:UTC");
  EXPECT_EQ(
      nativeLanceSemanticType(type->childAt(5)), "timestamp:ns:Asia/Shanghai");
  ASSERT_TRUE(nativeLanceSemanticType(type->childAt(7)).has_value());
  EXPECT_EQ(nativeLanceSemanticType(type->childAt(7))->substr(0, 5), "json:");

  const auto extended = load("extended_scalar_v2_2.lance");
  ASSERT_EQ(extended.metadata->rowType()->size(), 11);
  EXPECT_EQ(
      nativeLanceSemanticType(extended.metadata->rowType()->childAt(10)),
      "bfloat16");
}

class TestSemanticTypeAdapter final : public NativeLanceTypeAdapter {
 public:
  std::optional<TypePtr> resolve(const Request& request) const override {
    if (request.extensionName == "example.semantic.boolean") {
      sawField = request.fieldName == "flag";
      return request.storageType;
    }
    return std::nullopt;
  }

  mutable bool sawField{false};
};

TEST_F(NativeLanceTest, acceptsApplicationSemanticTypeAdapter) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(
          makeBooleanTimestampFile("example.semantic.boolean")),
      *pool_);
  const auto adapter = std::make_shared<TestSemanticTypeAdapter>();
  NativeLanceMetadata metadata(*input, *pool_, adapter);
  EXPECT_TRUE(adapter->sawField);
  EXPECT_EQ(metadata.rowType()->childAt(0)->kind(), TypeKind::BOOLEAN);
}

TEST_F(NativeLanceTest, parsesColonBearingDictionaryLogicalTypes) {
  EXPECT_EQ(
      nativeLanceDictionaryValueLogicalType("dict:date32:day:int32:false"),
      "date32:day");
  EXPECT_EQ(
      nativeLanceDictionaryValueLogicalType(
          "dict:timestamp:ns:Asia/Shanghai:uint64:true"),
      "timestamp:ns:Asia/Shanghai");
  EXPECT_EQ(
      nativeLanceDictionaryValueLogicalType("dict:decimal:128:30:4:int8:false"),
      "decimal:128:30:4");
  EXPECT_EQ(
      nativeLanceDictionaryValueLogicalType(
          "dict:fixed_size_binary:3:uint16:false"),
      "fixed_size_binary:3");
  EXPECT_THROW(
      nativeLanceDictionaryValueLogicalType(
          "dict:timestamp:ns:UTC:int128:false"),
      BoltException);
  EXPECT_THROW(
      nativeLanceDictionaryValueLogicalType("dict:int32:uint8:maybe"),
      BoltException);
}

TEST_F(NativeLanceTest, rejectsUnsupportedArrowTypeFamiliesExplicitly) {
  const std::array<std::pair<std::string_view, std::string_view>, 5> cases{{
      {"union:dense", "Union cannot be represented losslessly"},
      {"run_end_encoded:int32:int64",
       "RunEndEncoded cannot be represented losslessly"},
      {"interval:month_day_nano",
       "Interval has no stable file logical type contract"},
      {"list_view", "ListView cannot be represented"},
      {"large_list_view", "ListView cannot be represented"},
  }};
  for (const auto& [logicalType, expectedMessage] : cases) {
    auto input = std::make_unique<dwio::common::BufferedInput>(
        std::make_shared<InMemoryReadFile>(
            makeUnsupportedLogicalTypeFile(logicalType)),
        *pool_);
    EXPECT_THROW(
        {
          try {
            NativeLanceMetadata metadata(*input, *pool_);
          } catch (const BoltException& error) {
            EXPECT_NE(error.message().find(expectedMessage), std::string::npos);
            throw;
          }
        },
        BoltException);
  }
}

TEST_F(NativeLanceTest, decimalMetadata) {
  const auto file = load("decimal.lance");
  const auto& metadata = file.metadata;
  EXPECT_EQ(metadata->numRows(), 20);
  EXPECT_EQ(
      metadata->rowType()->toString(), "ROW<a:DECIMAL(5, 2),b:DECIMAL(20, 5)>");
  ASSERT_EQ(metadata->columns().size(), 2);
  EXPECT_EQ(metadata->columns()[0].pages(0).buffer_sizes(0), 320);
  EXPECT_EQ(metadata->columns()[1].pages(0).buffer_sizes(0), 320);
}

TEST_F(NativeLanceTest, preservesColumnNames) {
  const auto file = load("upper.lance");
  const auto& metadata = file.metadata;
  EXPECT_EQ(metadata->numRows(), 2);
  EXPECT_EQ(metadata->rowType()->toString(), "ROW<A:BIGINT,b:BIGINT>");
}

TEST_F(NativeLanceTest, decodesFixedWidthRange) {
  const auto file = load("sample.lance");
  const NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);

  const auto integers = decoder.decodeColumn(0, 5, 7);
  const auto* rawIntegers = integers->asFlatVector<int64_t>()->rawValues();
  const auto doubles = decoder.decodeColumn(1, 5, 7);
  const auto* rawDoubles = doubles->asFlatVector<double>()->rawValues();
  for (vector_size_t i = 0; i < 7; ++i) {
    EXPECT_FALSE(integers->isNullAt(i));
    EXPECT_EQ(rawIntegers[i], i + 6);
    EXPECT_FALSE(doubles->isNullAt(i));
    EXPECT_DOUBLE_EQ(rawDoubles[i], i + 6);
  }
}

TEST_F(NativeLanceTest, rangeReadDoesNotReadWholePage) {
  const std::string path = "examples/sample.lance";
  auto readFile = std::make_shared<LocalReadFile>(path);
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  const NativeLanceMetadata metadata(*input, *pool_);
  const auto bytesAfterMetadata = readFile->bytesRead();
  const NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto values = decoder.decodeColumn(0, 5, 2);
  EXPECT_EQ(values->asFlatVector<int64_t>()->valueAt(0), 6);
  EXPECT_EQ(values->asFlatVector<int64_t>()->valueAt(1), 7);
  EXPECT_EQ(readFile->bytesRead() - bytesAfterMetadata, 2 * sizeof(int64_t));
}

TEST_F(NativeLanceTest, decodesDecimal128) {
  const auto file = load("decimal.lance");
  const NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);

  const auto shortDecimals = decoder.decodeColumn(0, 3, 5);
  const auto* rawShort = shortDecimals->asFlatVector<int64_t>()->rawValues();
  const auto longDecimals = decoder.decodeColumn(1, 3, 5);
  const auto* rawLong = longDecimals->asFlatVector<int128_t>()->rawValues();
  const auto longBase = HugeInt::parse("10000000000000000000");
  for (vector_size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(rawShort[i], 10004 + i);
    EXPECT_EQ(rawLong[i], longBase + 4 + i);
  }
}

TEST_F(NativeLanceTest, parsesStableV2StringFixture) {
  const auto file = load("string_v2_0.lance");
  EXPECT_EQ(file.metadata->numRows(), 3);
  EXPECT_EQ(
      file.metadata->rowType()->toString(),
      "ROW<numbers:BIGINT,strings:VARCHAR,bins:VARBINARY,more_numbers:BIGINT>");
  EXPECT_EQ(file.metadata->columns().size(), 6);
  EXPECT_EQ(file.metadata->physicalColumnIndex(0), 0);
  EXPECT_EQ(file.metadata->physicalColumnIndex(1), 1);
  EXPECT_EQ(file.metadata->physicalColumnIndex(2), 3);
  EXPECT_EQ(file.metadata->physicalColumnIndex(3), 5);

  const NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
  const auto strings = decoder.decodeColumn(1, 1, 2);
  EXPECT_EQ(strings->asFlatVector<StringView>()->valueAt(0).str(), "bar");
  EXPECT_EQ(strings->asFlatVector<StringView>()->valueAt(1).str(), "baz");
  const auto binaries = decoder.decodeColumn(2, 0, 3);
  EXPECT_EQ(binaries->asFlatVector<StringView>()->valueAt(0).str(), "foo");
  EXPECT_EQ(binaries->asFlatVector<StringView>()->valueAt(1).str(), "bar");
  EXPECT_EQ(binaries->asFlatVector<StringView>()->valueAt(2).str(), "baz");
  const auto numbers = decoder.decodeColumn(3, 0, 3);
  EXPECT_EQ(numbers->asFlatVector<int64_t>()->valueAt(0), 4);
  EXPECT_EQ(numbers->asFlatVector<int64_t>()->valueAt(1), 5);
  EXPECT_EQ(numbers->asFlatVector<int64_t>()->valueAt(2), 6);
}

TEST_F(NativeLanceTest, decodesExactStructuralFixtures) {
  for (const auto* fileName : {"exact_v2_1.lance", "exact_v2_2.lance"}) {
    auto file = load(fileName);
    EXPECT_EQ(file.metadata->numRows(), 4'097);
    EXPECT_EQ(
        file.metadata->rowType()->toString(),
        "ROW<id:INTEGER,name:VARCHAR,items:ARRAY<INTEGER>,"
        "category:VARCHAR,blob:VARBINARY>");
    NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);

    const auto ids = decoder.decodeColumn(0, 1'020, 10);
    for (vector_size_t row = 0; row < ids->size(); ++row) {
      EXPECT_EQ(ids->asFlatVector<int32_t>()->valueAt(row), 1'020 + row);
    }
    const auto names = decoder.decodeColumn(1, 1'020, 10);
    for (vector_size_t row = 0; row < names->size(); ++row) {
      const auto absolute = 1'020 + row;
      EXPECT_EQ(names->isNullAt(row), absolute % 7 == 0);
      if (!names->isNullAt(row)) {
        EXPECT_EQ(
            names->asFlatVector<StringView>()->valueAt(row).str(),
            fmt::format("value-{:04}-deterministic-fixture", absolute));
      }
    }
    const auto items = decoder.decodeColumn(2, 1'020, 10);
    const auto* arrays = items->as<ArrayVector>();
    ASSERT_NE(arrays, nullptr);
    const auto* elements = arrays->elements()->asFlatVector<int32_t>();
    ASSERT_NE(elements, nullptr);
    for (vector_size_t row = 0; row < arrays->size(); ++row) {
      const auto absolute = 1'020 + row;
      EXPECT_EQ(arrays->isNullAt(row), absolute % 11 == 0);
      if (!arrays->isNullAt(row)) {
        ASSERT_EQ(arrays->sizeAt(row), 3);
        const auto offset = arrays->offsetAt(row);
        EXPECT_EQ(elements->valueAt(offset), absolute);
        EXPECT_EQ(elements->isNullAt(offset + 1), absolute % 5 == 0);
        EXPECT_EQ(elements->valueAt(offset + 2), absolute * 3);
      }
    }
  }
}

TEST_F(NativeLanceTest, structuralMiniBlockReadsSelectedChunks) {
  std::shared_ptr<ReadFile> source =
      std::make_shared<LocalReadFile>("examples/compression_v2_1.lance");
  const auto contents = source->pread(0, source->size());
  auto readFile = std::make_shared<CountingReadFile>(contents);
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata metadata(*input, *pool_);

  constexpr uint64_t kStart = 1'020;
  constexpr uint64_t kRows = 10;
  constexpr uint32_t kColumn = 1;
  const auto physical = metadata.physicalColumnIndex(kColumn);
  const auto& column = metadata.column(physical);
  ASSERT_GT(column.pages_size(), 0);
  const auto& page = column.pages(0);
  const auto& layout = metadata.pageLayout(physical, 0);
  ASSERT_TRUE(lanceStructuralPageSupportsRangeRead(
      metadata.rowType()->childAt(kColumn),
      {},
      metadata.physicalColumnChildLogicalTypes(physical),
      layout));
  ASSERT_EQ(
      layout.layout_case(), ::lance::encodings21::PageLayout::kMiniBlockLayout);

  const auto& mini = layout.mini_block_layout();
  const auto metadataWordBytes = mini.has_large_chunk() ? 4 : 2;
  ASSERT_EQ(page.buffer_sizes(0) % metadataWordBytes, 0);
  const auto numChunks = page.buffer_sizes(0) / metadataWordBytes;
  ASSERT_GT(numChunks, 1);
  uint64_t chunkStart = 0;
  std::optional<uint64_t> firstChunk;
  std::optional<uint64_t> lastChunk;
  for (uint64_t chunk = 0; chunk < numChunks; ++chunk) {
    const auto* word =
        contents.data() + page.buffer_offsets(0) + chunk * metadataWordBytes;
    const auto encoded = mini.has_large_chunk()
        ? static_cast<uint64_t>(
              folly::Endian::little(folly::loadUnaligned<uint32_t>(word)))
        : static_cast<uint64_t>(
              folly::Endian::little(folly::loadUnaligned<uint16_t>(word)));
    const auto chunkItems = chunk + 1 == numChunks
        ? mini.num_items() - chunkStart
        : uint64_t{1} << (encoded & 0xf);
    const auto chunkEnd = chunkStart + chunkItems;
    if (!firstChunk.has_value() && kStart < chunkEnd) {
      firstChunk = chunk;
    }
    if (!lastChunk.has_value() && kStart + kRows <= chunkEnd) {
      lastChunk = chunk;
    }
    chunkStart = chunkEnd;
  }
  ASSERT_TRUE(firstChunk.has_value());
  ASSERT_TRUE(lastChunk.has_value());
  EXPECT_LT(*firstChunk, *lastChunk);

  uint64_t fullPageBytes = 0;
  for (const auto bytes : page.buffer_sizes()) {
    fullPageBytes += bytes;
  }
  readFile->resetBytesRead();
  NativeLanceDecoder decoder(*input, metadata, *pool_);
  const auto values = decoder.decodeColumn(kColumn, kStart, kRows);
  for (vector_size_t row = 0; row < values->size(); ++row) {
    EXPECT_DOUBLE_EQ(
        values->asFlatVector<double>()->valueAt(row),
        1000.0 + (kStart + row) * 0.000'125);
  }
  EXPECT_LT(readFile->bytesRead(), fullPageBytes);

  std::shared_ptr<ReadFile> fallbackSource =
      std::make_shared<LocalReadFile>("examples/exact_v2_1.lance");
  auto fallbackReadFile = std::make_shared<CountingReadFile>(
      fallbackSource->pread(0, fallbackSource->size()));
  auto fallbackInput =
      std::make_unique<dwio::common::BufferedInput>(fallbackReadFile, *pool_);
  NativeLanceMetadata fallbackMetadata(*fallbackInput, *pool_);
  const auto namePhysical = fallbackMetadata.physicalColumnIndex(1);
  const auto& nameColumn = fallbackMetadata.column(namePhysical);
  ASSERT_GT(nameColumn.pages_size(), 0);
  const auto& nameLayout = fallbackMetadata.pageLayout(namePhysical, 0);
  EXPECT_FALSE(lanceStructuralPageSupportsRangeRead(
      fallbackMetadata.rowType()->childAt(1),
      {},
      fallbackMetadata.physicalColumnChildLogicalTypes(namePhysical),
      nameLayout));
  uint64_t fullNamePageBytes = 0;
  uint64_t namePageStart = 0;
  for (const auto& namePage : nameColumn.pages()) {
    const auto namePageEnd = namePageStart + namePage.length();
    if (kStart < namePageEnd && kStart + kRows > namePageStart) {
      for (const auto bytes : namePage.buffer_sizes()) {
        fullNamePageBytes += bytes;
      }
    }
    namePageStart = namePageEnd;
  }
  fallbackReadFile->resetBytesRead();
  NativeLanceDecoder fallbackDecoder(*fallbackInput, fallbackMetadata, *pool_);
  const auto names = fallbackDecoder.decodeColumn(1, kStart, kRows);
  ASSERT_EQ(names->size(), kRows);
  EXPECT_EQ(
      names->asFlatVector<StringView>()->valueAt(0).str(),
      "value-1020-deterministic-fixture");
  EXPECT_EQ(fallbackReadFile->bytesRead(), fullNamePageBytes);
}

TEST_F(NativeLanceTest, decodesStructuralComplexFixture) {
  auto file = load("complex_v2_2.lance");
  EXPECT_EQ(file.metadata->numRows(), 2'051);
  EXPECT_EQ(
      file.metadata->rowType()->toString(),
      "ROW<map_val:MAP<INTEGER,BIGINT>,fsl:ARRAY<INTEGER>,"
      "fsl_struct:ARRAY<ROW<x:INTEGER,y:VARCHAR>>>");
  NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);

  constexpr vector_size_t kStart = 1'020;
  constexpr vector_size_t kRows = 17;
  const auto maps = decoder.decodeColumn(0, kStart, kRows);
  const auto* map = maps->as<MapVector>();
  ASSERT_NE(map, nullptr);
  for (vector_size_t row = 0; row < kRows; ++row) {
    const auto absolute = kStart + row;
    EXPECT_EQ(map->isNullAt(row), absolute % 17 == 0);
    if (!map->isNullAt(row)) {
      EXPECT_EQ(map->sizeAt(row), absolute % 4);
      for (vector_size_t item = 0; item < map->sizeAt(row); ++item) {
        const auto index = map->offsetAt(row) + item;
        EXPECT_EQ(
            map->mapKeys()->asFlatVector<int32_t>()->valueAt(index),
            absolute * 10 + item);
        EXPECT_EQ(
            map->mapValues()->isNullAt(index), item == 1 && absolute % 5 == 0);
      }
    }
  }

  const auto fixed = decoder.decodeColumn(1, kStart, kRows);
  const auto* fixedArray = fixed->as<ArrayVector>();
  ASSERT_NE(fixedArray, nullptr);
  for (vector_size_t row = 0; row < kRows; ++row) {
    const auto absolute = kStart + row;
    EXPECT_EQ(fixedArray->isNullAt(row), absolute % 31 == 0);
    if (fixedArray->isNullAt(row)) {
      EXPECT_EQ(fixedArray->sizeAt(row), 0);
      continue;
    }
    EXPECT_EQ(fixedArray->sizeAt(row), 3);
    for (vector_size_t item = 0; item < 3; ++item) {
      const auto valueIndex = fixedArray->offsetAt(row) + item;
      const auto absoluteItem = absolute * 3 + item;
      EXPECT_EQ(
          fixedArray->elements()->isNullAt(valueIndex), absoluteItem % 29 == 0);
      if (!fixedArray->elements()->isNullAt(valueIndex)) {
        EXPECT_EQ(
            fixedArray->elements()->asFlatVector<int32_t>()->valueAt(
                valueIndex),
            absoluteItem - 1000);
      }
    }
  }

  const auto fixedStruct = decoder.decodeColumn(2, kStart, kRows);
  const auto* outer = fixedStruct->as<ArrayVector>();
  ASSERT_NE(outer, nullptr);
  const auto* rows = outer->elements()->as<RowVector>();
  ASSERT_NE(rows, nullptr);
  for (vector_size_t row = 0; row < kRows; ++row) {
    const auto absolute = kStart + row;
    EXPECT_EQ(outer->isNullAt(row), absolute % 23 == 0);
    if (outer->isNullAt(row)) {
      EXPECT_EQ(outer->sizeAt(row), 0);
      continue;
    }
    EXPECT_EQ(outer->sizeAt(row), 2);
    for (vector_size_t item = 0; item < 2; ++item) {
      const auto valueIndex = outer->offsetAt(row) + item;
      const auto absoluteItem = absolute * 2 + item;
      EXPECT_EQ(rows->isNullAt(valueIndex), absoluteItem % 19 == 0);
      EXPECT_EQ(rows->childAt(0)->isNullAt(valueIndex), absoluteItem % 11 == 0);
      EXPECT_EQ(rows->childAt(1)->isNullAt(valueIndex), absoluteItem % 13 == 0);
    }
  }
}

TEST_F(NativeLanceTest, decodesFixedSizeListStructWithListAndMap) {
  auto file = load("complex_fsl_struct_v2_2.lance");
  EXPECT_EQ(
      file.metadata->rowType()->toString(),
      "ROW<complex_fsl_miniblock:ARRAY<ROW<id:INTEGER,items:ARRAY<INTEGER>,"
      "attrs:MAP<INTEGER,BIGINT>>>,complex_fsl_fullzip:ARRAY<ROW<id:INTEGER,"
      "items:ARRAY<INTEGER>,attrs:MAP<INTEGER,BIGINT>>>>");
  NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
  constexpr vector_size_t kStart = 1'020;
  constexpr vector_size_t kRows = 17;
  for (uint32_t column = 0; column < 2; ++column) {
    const auto decoded = decoder.decodeColumn(column, kStart, kRows);
    const auto* outer = decoded->as<ArrayVector>();
    ASSERT_NE(outer, nullptr);
    ASSERT_EQ(outer->size(), kRows);
    const auto* records = outer->elements()->as<RowVector>();
    ASSERT_NE(records, nullptr);
    const auto* ids = records->childAt(0)->asFlatVector<int32_t>();
    const auto* lists = records->childAt(1)->as<ArrayVector>();
    const auto* maps = records->childAt(2)->as<MapVector>();
    ASSERT_NE(ids, nullptr);
    ASSERT_NE(lists, nullptr);
    ASSERT_NE(maps, nullptr);
    for (vector_size_t row = 0; row < kRows; ++row) {
      const auto absolute = kStart + row;
      EXPECT_EQ(outer->isNullAt(row), absolute % 19 == 0);
      if (outer->isNullAt(row)) {
        continue;
      }
      EXPECT_EQ(outer->sizeAt(row), 2);
      for (vector_size_t item = 0; item < 2; ++item) {
        const auto slot = absolute * 2 + item;
        const auto childRow = outer->offsetAt(row) + item;
        EXPECT_EQ(records->isNullAt(childRow), slot % 17 == 0);
        if (records->isNullAt(childRow)) {
          continue;
        }
        EXPECT_EQ(ids->isNullAt(childRow), slot % 7 == 0);
        if (!ids->isNullAt(childRow)) {
          EXPECT_EQ(ids->valueAt(childRow), slot - 1000);
        }
        EXPECT_EQ(lists->isNullAt(childRow), slot % 11 == 0);
        EXPECT_EQ(lists->sizeAt(childRow), slot % 11 == 0 ? 0 : slot % 3);
        for (vector_size_t listItem = 0; listItem < lists->sizeAt(childRow);
             ++listItem) {
          const auto element = lists->offsetAt(childRow) + listItem;
          const auto* values = lists->elements()->asFlatVector<int32_t>();
          EXPECT_EQ(values->isNullAt(element), listItem == 1 && slot % 5 == 0);
          if (!values->isNullAt(element)) {
            EXPECT_EQ(values->valueAt(element), slot * 10 + listItem);
          }
        }
        EXPECT_EQ(maps->isNullAt(childRow), slot % 13 == 0);
        EXPECT_EQ(maps->sizeAt(childRow), slot % 13 == 0 ? 0 : slot % 3);
        for (vector_size_t mapItem = 0; mapItem < maps->sizeAt(childRow);
             ++mapItem) {
          const auto element = maps->offsetAt(childRow) + mapItem;
          EXPECT_EQ(
              maps->mapKeys()->asFlatVector<int32_t>()->valueAt(element),
              slot * 10 + mapItem);
          const auto* values = maps->mapValues()->asFlatVector<int64_t>();
          EXPECT_EQ(values->isNullAt(element), mapItem == 1 && slot % 5 == 0);
          if (!values->isNullAt(element)) {
            EXPECT_EQ(values->valueAt(element), slot * 100 + mapItem);
          }
        }
      }
    }
  }
}

TEST_F(NativeLanceTest, decodesStructuralScalarFixture) {
  for (const auto* fileName : {"scalar_v2_1.lance", "scalar_v2_2.lance"}) {
    auto file = load(fileName);
    EXPECT_EQ(
        file.metadata->rowType()->toString(),
        "ROW<decimal:DECIMAL(20, 2),"
        "timestamp:LANCE_timestamp:us:Asia/Shanghai,fixed:VARBINARY,"
        "wide_string:VARCHAR>");
    NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
    constexpr vector_size_t kStart = 1'020;
    constexpr vector_size_t kRows = 17;

    const auto decimals = decoder.decodeColumn(0, kStart, kRows);
    const auto timestamps = decoder.decodeColumn(1, kStart, kRows);
    const auto fixed = decoder.decodeColumn(2, kStart, kRows);
    const auto strings = decoder.decodeColumn(3, kStart, kRows);
    for (vector_size_t row = 0; row < kRows; ++row) {
      const auto absolute = kStart + row;
      EXPECT_EQ(decimals->isNullAt(row), absolute % 19 == 0);
      if (!decimals->isNullAt(row)) {
        EXPECT_EQ(
            decimals->asFlatVector<int128_t>()->valueAt(row),
            (static_cast<int128_t>(absolute) - 1000) * 100 + 7);
      }
      EXPECT_EQ(timestamps->isNullAt(row), absolute % 23 == 0);
      if (!timestamps->isNullAt(row)) {
        EXPECT_EQ(
            timestamps->asFlatVector<Timestamp>()->valueAt(row),
            Timestamp::fromMicros((absolute - 1000) * 1'000'001));
      }
      EXPECT_EQ(fixed->isNullAt(row), absolute % 29 == 0);
      if (!fixed->isNullAt(row)) {
        const auto bytes = fixed->asFlatVector<StringView>()->valueAt(row);
        ASSERT_EQ(bytes.size(), 3);
        EXPECT_EQ(static_cast<uint8_t>(bytes.data()[0]), absolute & 0xff);
        EXPECT_EQ(static_cast<uint8_t>(bytes.data()[1]), absolute >> 8);
        EXPECT_EQ(static_cast<uint8_t>(bytes.data()[2]), 0x5a);
      }
      EXPECT_EQ(strings->isNullAt(row), absolute % 31 == 0);
      if (!strings->isNullAt(row)) {
        EXPECT_EQ(
            strings->asFlatVector<StringView>()->valueAt(row).str(),
            fmt::format("{}-{}", std::string(384, 'x'), absolute));
      }
    }
  }
}

TEST_F(NativeLanceTest, decodesSparseV23Fixture) {
  auto file = load("sparse_v2_3.lance");
  EXPECT_EQ(
      file.metadata->rowType()->toString(),
      "ROW<primitive:BIGINT,list:ARRAY<INTEGER>,fixed:ARRAY<INTEGER>>");
  NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
  constexpr vector_size_t kStart = 1'020;
  constexpr vector_size_t kRows = 17;

  const auto primitive = decoder.decodeColumn(0, kStart, kRows);
  const auto lists = decoder.decodeColumn(1, kStart, kRows);
  const auto fixed = decoder.decodeColumn(2, kStart, kRows);
  const auto* listVector = lists->as<ArrayVector>();
  const auto* fixedVector = fixed->as<ArrayVector>();
  ASSERT_NE(listVector, nullptr);
  ASSERT_NE(fixedVector, nullptr);
  const auto* listValues = listVector->elements()->asFlatVector<int32_t>();
  const auto* fixedValues = fixedVector->elements()->asFlatVector<int32_t>();
  ASSERT_NE(listValues, nullptr);
  ASSERT_NE(fixedValues, nullptr);

  for (vector_size_t row = 0; row < kRows; ++row) {
    const auto absolute = kStart + row;
    EXPECT_EQ(
        primitive->isNullAt(row), absolute % 17 == 0 || absolute % 29 == 0);
    if (!primitive->isNullAt(row)) {
      EXPECT_EQ(
          primitive->asFlatVector<int64_t>()->valueAt(row),
          absolute * 101 - 7000);
    }

    EXPECT_EQ(lists->isNullAt(row), absolute % 19 == 0);
    const auto expectedListSize = absolute % 19 == 0 ? 0 : absolute % 5;
    EXPECT_EQ(listVector->sizeAt(row), expectedListSize);
    for (vector_size_t item = 0; item < expectedListSize; ++item) {
      const auto element = listVector->offsetAt(row) + item;
      EXPECT_EQ(listValues->isNullAt(element), item == 1 && absolute % 7 == 0);
      if (!listValues->isNullAt(element)) {
        EXPECT_EQ(listValues->valueAt(element), absolute * 10 + item);
      }
    }

    EXPECT_EQ(fixed->isNullAt(row), absolute % 31 == 0);
    EXPECT_EQ(fixedVector->sizeAt(row), absolute % 31 == 0 ? 0 : 3);
    if (fixed->isNullAt(row)) {
      continue;
    }
    for (vector_size_t item = 0; item < 3; ++item) {
      const auto slot = absolute * 3 + item;
      const auto element = fixedVector->offsetAt(row) + item;
      EXPECT_EQ(fixedValues->isNullAt(element), slot % 23 == 0);
      if (!fixedValues->isNullAt(element)) {
        EXPECT_EQ(fixedValues->valueAt(element), slot - 2000);
      }
    }
  }
}

TEST_F(NativeLanceTest, lazilyLoadsProjectedStructuralColumnMetadata) {
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader projectedReader(
      openFile("sparse_v2_3.lance", *pool_), readerOptions);
  EXPECT_EQ(projectedReader.loadedColumnMetadataCount(), 0);

  dwio::common::RowReaderOptions projectedOptions;
  projectedOptions.select(std::make_shared<dwio::common::ColumnSelector>(
      projectedReader.rowType(), std::vector<std::string>{"primitive"}));
  auto rowReader = projectedReader.createRowReader(projectedOptions);
  EXPECT_EQ(projectedReader.loadedColumnMetadataCount(), 1);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(8, result), 8);
  EXPECT_EQ(projectedReader.loadedColumnMetadataCount(), 1);
  const auto* values =
      result->as<RowVector>()->childAt(0)->asFlatVector<int64_t>();
  ASSERT_NE(values, nullptr);
  EXPECT_TRUE(values->isNullAt(0));
  EXPECT_EQ(values->valueAt(1), 1 * 101 - 7000);

  NativeLanceReader fullReader(
      openFile("sparse_v2_3.lance", *pool_), readerOptions);
  EXPECT_EQ(fullReader.loadedColumnMetadataCount(), 0);
  auto fullRowReader =
      fullReader.createRowReader(dwio::common::RowReaderOptions{});
  EXPECT_EQ(fullReader.loadedColumnMetadataCount(), 3);
  EXPECT_EQ(fullRowReader->next(1, result), 1);
}

TEST_F(NativeLanceTest, decodesStructuralFixedFullZipFixture) {
  for (const auto* fileName :
       {"fixed_fullzip_v2_1.lance", "fixed_fullzip_v2_2.lance"}) {
    auto file = load(fileName);
    EXPECT_EQ(file.metadata->rowType()->toString(), "ROW<value:BIGINT>");
    NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
    constexpr vector_size_t kStart = 1'020;
    constexpr vector_size_t kRows = 17;
    const auto values = decoder.decodeColumn(0, kStart, kRows);
    for (vector_size_t row = 0; row < kRows; ++row) {
      const auto absolute = kStart + row;
      EXPECT_EQ(values->isNullAt(row), absolute % 17 == 0);
      if (!values->isNullAt(row)) {
        EXPECT_EQ(
            values->asFlatVector<int64_t>()->valueAt(row),
            (absolute - 1000) * 97);
      }
    }
  }
}

TEST_F(NativeLanceTest, decodesStructuralCompressionFixtures) {
  for (const auto* fileName :
       {"compression_v2_1.lance", "compression_v2_2.lance"}) {
    auto file = load(fileName);
    EXPECT_EQ(
        file.metadata->rowType()->toString(),
        "ROW<rle:INTEGER,bss:DOUBLE,general:BIGINT>");
    NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
    constexpr vector_size_t kStart = 1'020;
    constexpr vector_size_t kRows = 17;
    const auto rle = decoder.decodeColumn(0, kStart, kRows);
    const auto bss = decoder.decodeColumn(1, kStart, kRows);
    const auto general = decoder.decodeColumn(2, kStart, kRows);
    for (vector_size_t row = 0; row < kRows; ++row) {
      const auto absolute = kStart + row;
      EXPECT_EQ(rle->asFlatVector<int32_t>()->valueAt(row), absolute / 64);
      EXPECT_DOUBLE_EQ(
          bss->asFlatVector<double>()->valueAt(row),
          1000.0 + absolute * 0.000'125);
      EXPECT_EQ(
          general->as<SimpleVector<int64_t>>()->valueAt(row),
          static_cast<int64_t>(absolute / 8) << 24);
    }
  }
}

TEST_F(NativeLanceTest, decodesStructuralExtendedScalarFixtures) {
  for (const auto* fileName :
       {"extended_scalar_v2_1.lance", "extended_scalar_v2_2.lance"}) {
    auto file = load(fileName);
    EXPECT_EQ(
        file.metadata->rowType()->toString(),
        "ROW<float16:REAL,date32:DATE,date64:DATE,"
        "time32:LANCE_time32:s,time64:LANCE_time64:ns,"
        "duration_s:INTERVAL DAY TO SECOND,"
        "duration_us:INTERVAL DAY TO SECOND,duration_ns:INTERVAL DAY TO "
        "SECOND,large_binary:VARBINARY,all_null:UNKNOWN,"
        "bfloat:LANCE_bfloat16>");
    NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
    constexpr vector_size_t kStart = 1'020;
    constexpr vector_size_t kRows = 17;
    const auto f16 = decoder.decodeColumn(0, kStart, kRows);
    const auto date32 = decoder.decodeColumn(1, kStart, kRows);
    const auto date64 = decoder.decodeColumn(2, kStart, kRows);
    const auto time32 = decoder.decodeColumn(3, kStart, kRows);
    const auto time64 = decoder.decodeColumn(4, kStart, kRows);
    const auto durationS = decoder.decodeColumn(5, kStart, kRows);
    const auto durationUs = decoder.decodeColumn(6, kStart, kRows);
    const auto durationNs = decoder.decodeColumn(7, kStart, kRows);
    const auto binary = decoder.decodeColumn(8, kStart, kRows);
    const auto allNull = decoder.decodeColumn(9, kStart, kRows);
    const auto bfloat = decoder.decodeColumn(10, kStart, kRows);
    for (vector_size_t row = 0; row < kRows; ++row) {
      const auto absolute = kStart + row;
      EXPECT_EQ(f16->isNullAt(row), absolute % 17 == 0);
      if (!f16->isNullAt(row)) {
        EXPECT_FLOAT_EQ(
            f16->asFlatVector<float>()->valueAt(row), absolute * 0.25f - 10);
      }
      EXPECT_EQ(date32->isNullAt(row), absolute % 19 == 0);
      if (!date32->isNullAt(row)) {
        EXPECT_EQ(
            date32->asFlatVector<int32_t>()->valueAt(row), absolute - 1000);
      }
      EXPECT_EQ(date64->isNullAt(row), absolute % 23 == 0);
      if (!date64->isNullAt(row)) {
        EXPECT_EQ(
            date64->asFlatVector<int32_t>()->valueAt(row), absolute - 1000);
      }
      EXPECT_EQ(time32->isNullAt(row), absolute % 29 == 0);
      if (!time32->isNullAt(row)) {
        EXPECT_EQ(
            time32->asFlatVector<int64_t>()->valueAt(row), absolute % 86'400);
      }
      EXPECT_EQ(time64->isNullAt(row), absolute % 31 == 0);
      if (!time64->isNullAt(row)) {
        EXPECT_EQ(
            time64->asFlatVector<int64_t>()->valueAt(row),
            absolute * 1'000'003);
      }
      EXPECT_EQ(durationS->isNullAt(row), absolute % 37 == 0);
      if (!durationS->isNullAt(row)) {
        EXPECT_EQ(
            durationS->asFlatVector<int64_t>()->valueAt(row),
            (absolute - 1000) * 1000);
      }
      EXPECT_EQ(durationUs->isNullAt(row), absolute % 41 == 0);
      if (!durationUs->isNullAt(row)) {
        EXPECT_EQ(
            durationUs->asFlatVector<int64_t>()->valueAt(row), absolute - 1000);
      }
      EXPECT_EQ(durationNs->isNullAt(row), absolute % 43 == 0);
      if (!durationNs->isNullAt(row)) {
        EXPECT_EQ(
            durationNs->asFlatVector<int64_t>()->valueAt(row), absolute - 1000);
      }
      EXPECT_EQ(binary->isNullAt(row), absolute % 47 == 0);
      if (!binary->isNullAt(row)) {
        EXPECT_EQ(
            binary->asFlatVector<StringView>()->valueAt(row).str(),
            fmt::format("large-binary-{}", absolute));
      }
      EXPECT_TRUE(allNull->isNullAt(row));
      EXPECT_EQ(bfloat->isNullAt(row), absolute % 53 == 0);
      if (!bfloat->isNullAt(row)) {
        const auto value = bfloat->asFlatVector<StringView>()->valueAt(row);
        ASSERT_EQ(value.size(), 2);
        EXPECT_EQ(
            folly::Endian::little(folly::loadUnaligned<uint16_t>(value.data())),
            absolute);
      }
    }
  }
}

TEST_F(NativeLanceTest, decodesStructuralPackedStructFixtures) {
  for (const auto* fileName :
       {"packed_fixed_v2_1.lance", "packed_fixed_v2_2.lance"}) {
    auto file = load(fileName);
    EXPECT_EQ(
        file.metadata->rowType()->toString(),
        "ROW<packed:ROW<x:INTEGER,y:BIGINT>>");
    NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
    const auto decoded = decoder.decodeColumn(0, 1'020, 17);
    const auto* rows = decoded->as<RowVector>();
    ASSERT_NE(rows, nullptr);
    for (vector_size_t row = 0; row < decoded->size(); ++row) {
      const auto absolute = 1'020 + row;
      EXPECT_EQ(decoded->isNullAt(row), absolute % 19 == 0);
      if (!decoded->isNullAt(row)) {
        EXPECT_EQ(
            rows->childAt(0)->asFlatVector<int32_t>()->valueAt(row), absolute);
        EXPECT_EQ(
            rows->childAt(1)->asFlatVector<int64_t>()->valueAt(row),
            absolute * 101);
      }
    }
  }

  auto file = load("packed_variable_v2_2.lance");
  EXPECT_EQ(
      file.metadata->rowType()->toString(),
      "ROW<packed:ROW<x:INTEGER,y:VARCHAR>>");
  NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
  const auto decoded = decoder.decodeColumn(0, 1'020, 17);
  const auto* rows = decoded->as<RowVector>();
  ASSERT_NE(rows, nullptr);
  for (vector_size_t row = 0; row < decoded->size(); ++row) {
    const auto absolute = 1'020 + row;
    EXPECT_EQ(decoded->isNullAt(row), absolute % 19 == 0);
    if (!decoded->isNullAt(row)) {
      EXPECT_EQ(
          rows->childAt(0)->asFlatVector<int32_t>()->valueAt(row), absolute);
      EXPECT_EQ(
          rows->childAt(1)->asFlatVector<StringView>()->valueAt(row).str(),
          fmt::format("packed-{}", absolute));
    }
  }
}

TEST_F(NativeLanceTest, decodesInlineBlobV2AcrossChunkBoundary) {
  auto file = load("blob_v2.lance");
  EXPECT_EQ(file.metadata->numRows(), 2'051);
  EXPECT_EQ(file.metadata->rowType()->toString(), "ROW<blob:VARBINARY>");
  EXPECT_EQ(file.metadata->columnLogicalType(0), "lance.blob.v2");
  EXPECT_EQ(file.metadata->physicalColumnLogicalType(0), "lance.blob.v2");
  EXPECT_EQ(
      file.metadata->physicalColumnChildLogicalTypes(0),
      (std::vector<std::string>{
          "uint8", "uint64", "uint64", "uint32", "string"}));

  NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
  constexpr vector_size_t kStart = 1'020;
  constexpr vector_size_t kRows = 17;
  decoder.prefetchColumns({0}, kStart, kRows);
  const auto decoded = decoder.decodeColumn(0, kStart, kRows);
  ASSERT_EQ(decoded->size(), kRows);
  const auto* values = decoded->asFlatVector<StringView>();
  ASSERT_NE(values, nullptr);
  for (vector_size_t row = 0; row < kRows; ++row) {
    const auto absolute = kStart + row;
    EXPECT_EQ(decoded->isNullAt(row), absolute % 17 == 0);
    if (decoded->isNullAt(row)) {
      continue;
    }
    const auto expected = absolute % 19 == 0
        ? std::string{}
        : fmt::format("blob-v2-inline-{}", absolute);
    EXPECT_EQ(values->valueAt(row).str(), expected);
  }
}

TEST_F(NativeLanceTest, rejectsBlobV2ThatRequiresDatasetResolver) {
  auto file = load("blob_v2_non_inline.lance");
  NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
  EXPECT_THROW(
      {
        try {
          decoder.decodeColumn(0, 1'024, 1);
        } catch (const BoltException& error) {
          EXPECT_NE(
              error.message().find(
                  "requires a dataset/sidecar object resolver"),
              std::string::npos);
          throw;
        }
      },
      BoltException);
}

TEST_F(NativeLanceTest, resolvesAllBlobV2StorageKinds) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeBlobResolverFile()), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  auto resolver = std::make_shared<TestBlobResolver>();
  NativeLanceDecoder decoder(*input, metadata, *pool_, false, resolver);
  const auto decoded = decoder.decodeColumn(0, 0, 4);
  const auto* values = decoded->asFlatVector<StringView>();
  ASSERT_NE(values, nullptr);
  ASSERT_EQ(values->size(), 4);
  EXPECT_EQ(values->valueAt(0).str(), "inline");
  EXPECT_EQ(values->valueAt(1).str(), "abc");
  EXPECT_EQ(values->valueAt(2).str(), "DEDI");
  EXPECT_EQ(values->valueAt(3).str(), "external");
  ASSERT_EQ(resolver->requests.size(), 3);
  EXPECT_EQ(resolver->requests[0].kind, NativeLanceBlobResolver::Kind::kPacked);
  EXPECT_EQ(resolver->requests[0].position, 1);
  EXPECT_EQ(resolver->requests[0].size, 3);
  EXPECT_EQ(
      resolver->requests[1].kind, NativeLanceBlobResolver::Kind::kDedicated);
  EXPECT_EQ(resolver->requests[1].position, 0);
  EXPECT_EQ(resolver->requests[1].size, 4);
  EXPECT_EQ(
      resolver->requests[2].kind, NativeLanceBlobResolver::Kind::kExternal);
  EXPECT_EQ(resolver->requests[2].position, 2);
  EXPECT_EQ(resolver->requests[2].size, 0);
}

TEST_F(NativeLanceTest, cachesResolvedBlobObjectsAcrossBatches) {
  auto delegate = std::make_shared<TestBlobResolver>();
  CachingNativeLanceBlobResolver resolver(delegate, 1);
  NativeLanceBlobResolver::Request packed{
      .kind = NativeLanceBlobResolver::Kind::kPacked,
      .sourceDataFile = "data.lance",
      .blobId = 7,
      .uri = "",
      .position = 1,
      .size = 3};

  auto first = resolver.resolve(packed, *pool_);
  packed.position = 5;
  packed.size = 2;
  auto second = resolver.resolve(packed, *pool_);
  EXPECT_EQ(delegate->requests.size(), 1);
  EXPECT_NE(first.get(), second.get());
  EXPECT_EQ(first->getReadFile().get(), second->getReadFile().get());

  auto dedicated = packed;
  dedicated.kind = NativeLanceBlobResolver::Kind::kDedicated;
  dedicated.blobId = 8;
  resolver.resolve(dedicated, *pool_);
  EXPECT_EQ(delegate->requests.size(), 2);
  resolver.resolve(packed, *pool_);
  EXPECT_EQ(delegate->requests.size(), 3);
}

TEST_F(NativeLanceTest, rowReaderPropagatesBlobV2Resolver) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeBlobResolverFile()), *pool_);
  auto resolver = std::make_shared<TestBlobResolver>();
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReaderFactory factory(resolver);
  auto reader = factory.createReader(std::move(input), readerOptions);
  auto rowReader = reader->createRowReader();
  auto prefetch = rowReader->prefetchUnits();
  ASSERT_TRUE(prefetch.has_value());
  ASSERT_EQ(prefetch->size(), 1);
  EXPECT_EQ(
      prefetch->front().prefetch(),
      dwio::common::RowReader::FetchResult::kFetched);
  VectorPtr result;
  EXPECT_EQ(rowReader->next(4, result), 4);
  const auto* rows = result->as<RowVector>();
  ASSERT_NE(rows, nullptr);
  const auto* values = rows->childAt(0)->asFlatVector<StringView>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0).str(), "inline");
  EXPECT_EQ(values->valueAt(1).str(), "abc");
  EXPECT_EQ(values->valueAt(2).str(), "DEDI");
  EXPECT_EQ(values->valueAt(3).str(), "external");
  EXPECT_EQ(resolver->requests.size(), 3);
}

TEST_F(NativeLanceTest, decodesStructuralTypeMatrixFixtures) {
  for (const auto* fileName :
       {"type_matrix_v2_1.lance", "type_matrix_v2_2.lance"}) {
    auto file = load(fileName);
    EXPECT_EQ(
        file.metadata->rowType()->toString(),
        "ROW<time32_ms:LANCE_time32:ms,time64_us:LANCE_time64:us,"
        "duration_ms:INTERVAL DAY TO SECOND,timestamp_s:TIMESTAMP,"
        "timestamp_ms:LANCE_timestamp:ms:UTC,"
        "timestamp_ns:LANCE_timestamp:ns:Asia/Shanghai,"
        "large_string:VARCHAR,json:LANCE_json:lance.json>");
    NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
    constexpr vector_size_t kStart = 1'020;
    constexpr vector_size_t kRows = 17;
    const auto time32 = decoder.decodeColumn(0, kStart, kRows);
    const auto time64 = decoder.decodeColumn(1, kStart, kRows);
    const auto duration = decoder.decodeColumn(2, kStart, kRows);
    const auto timestampS = decoder.decodeColumn(3, kStart, kRows);
    const auto timestampMs = decoder.decodeColumn(4, kStart, kRows);
    const auto timestampNs = decoder.decodeColumn(5, kStart, kRows);
    const auto largeString = decoder.decodeColumn(6, kStart, kRows);
    const auto json = decoder.decodeColumn(7, kStart, kRows);
    for (vector_size_t row = 0; row < kRows; ++row) {
      const auto absolute = kStart + row;
      EXPECT_EQ(time32->isNullAt(row), absolute % 17 == 0);
      if (!time32->isNullAt(row)) {
        EXPECT_EQ(
            time32->asFlatVector<int64_t>()->valueAt(row), absolute * 101);
      }
      EXPECT_EQ(time64->isNullAt(row), absolute % 19 == 0);
      if (!time64->isNullAt(row)) {
        EXPECT_EQ(
            time64->asFlatVector<int64_t>()->valueAt(row), absolute * 100'003);
      }
      EXPECT_EQ(duration->isNullAt(row), absolute % 23 == 0);
      if (!duration->isNullAt(row)) {
        EXPECT_EQ(
            duration->asFlatVector<int64_t>()->valueAt(row), absolute - 1000);
      }
      EXPECT_EQ(timestampS->isNullAt(row), absolute % 29 == 0);
      if (!timestampS->isNullAt(row)) {
        EXPECT_EQ(
            timestampS->asFlatVector<Timestamp>()->valueAt(row),
            Timestamp(absolute - 1000, 0));
      }
      EXPECT_EQ(timestampMs->isNullAt(row), absolute % 31 == 0);
      if (!timestampMs->isNullAt(row)) {
        EXPECT_EQ(
            timestampMs->asFlatVector<Timestamp>()->valueAt(row),
            Timestamp::fromMillis((absolute - 1000) * 1001));
      }
      EXPECT_EQ(timestampNs->isNullAt(row), absolute % 37 == 0);
      if (!timestampNs->isNullAt(row)) {
        EXPECT_EQ(
            timestampNs->asFlatVector<Timestamp>()->valueAt(row),
            Timestamp::fromNanos((absolute - 1000) * 1'000'003));
      }
      EXPECT_EQ(largeString->isNullAt(row), absolute % 41 == 0);
      if (!largeString->isNullAt(row)) {
        EXPECT_EQ(
            largeString->asFlatVector<StringView>()->valueAt(row).str(),
            fmt::format("large-string-{}", absolute));
      }
      EXPECT_EQ(json->isNullAt(row), absolute % 43 == 0);
      if (!json->isNullAt(row)) {
        // Lance stores its JSON extension as JSONB. The reader intentionally
        // exposes the lossless storage bytes because Bolt has no JSON scalar.
        EXPECT_GT(json->asFlatVector<StringView>()->valueAt(row).size(), 0);
      }
    }
  }
}

TEST_F(NativeLanceTest, decodesStructuralDictionaryTypeMatrix) {
  for (const auto* fileName :
       {"dictionary_v2_1.lance", "dictionary_v2_2.lance"}) {
    auto file = load(fileName);
    EXPECT_EQ(
        file.metadata->rowType()->toString(),
        "ROW<dict_i8_string:VARCHAR,dict_u8_string:VARCHAR,"
        "dict_i16_bigint:BIGINT,dict_u16_binary:VARBINARY,"
        "dict_u32_double:DOUBLE,dict_u64_string:VARCHAR,"
        "dict_i32_bigint:BIGINT,dict_i64_string:VARCHAR>");
    NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
    constexpr vector_size_t kStart = 1'020;
    constexpr vector_size_t kRows = 17;
    std::vector<VectorPtr> columns;
    for (uint32_t column = 0; column < 8; ++column) {
      columns.push_back(decoder.decodeColumn(column, kStart, kRows));
    }
    constexpr std::array<std::string_view, 4> kStrings{
        "zero", "one", "two", "three"};
    constexpr std::array<std::string_view, 3> kBinary{"aa", "bbb", ""};
    constexpr std::array<int64_t, 3> kInt64{111, -222, 333};
    constexpr std::array<double, 3> kDouble{1.25, -2.5, 9.75};
    for (vector_size_t row = 0; row < kRows; ++row) {
      const auto absolute = kStart + row;
      EXPECT_EQ(columns[0]->isNullAt(row), absolute % 17 == 0);
      if (!columns[0]->isNullAt(row)) {
        EXPECT_EQ(
            columns[0]->as<SimpleVector<StringView>>()->valueAt(row).str(),
            kStrings[absolute % 4]);
      }
      EXPECT_EQ(
          columns[1]->as<SimpleVector<StringView>>()->valueAt(row).str(),
          kStrings[absolute % 4]);
      EXPECT_EQ(
          columns[2]->as<SimpleVector<int64_t>>()->valueAt(row),
          kInt64[absolute % 3]);
      EXPECT_EQ(
          columns[3]->as<SimpleVector<StringView>>()->valueAt(row).str(),
          kBinary[absolute % 3]);
      EXPECT_DOUBLE_EQ(
          columns[4]->as<SimpleVector<double>>()->valueAt(row),
          kDouble[absolute % 3]);
      EXPECT_EQ(
          columns[5]->as<SimpleVector<StringView>>()->valueAt(row).str(),
          kStrings[absolute % 4]);
      EXPECT_EQ(
          columns[6]->as<SimpleVector<int64_t>>()->valueAt(row),
          static_cast<int64_t>(7 + absolute % 3));
      EXPECT_EQ(
          columns[7]->as<SimpleVector<StringView>>()->valueAt(row).str(),
          std::string(1, "abcd"[absolute % 4]));
    }
  }
}

TEST_F(NativeLanceTest, decodesAllSupportedDictionaryValueTypes) {
  for (const auto* fileName :
       {"dictionary_values_v2_1.lance", "dictionary_values_v2_2.lance"}) {
    auto file = load(fileName);
    EXPECT_EQ(
        file.metadata->rowType()->toString(),
        "ROW<dict_bool:BOOLEAN,dict_i8:TINYINT,dict_u8:SMALLINT,"
        "dict_i16:SMALLINT,dict_u16:INTEGER,dict_i32:INTEGER,"
        "dict_u32:BIGINT,dict_i64:BIGINT,dict_u64:HUGEINT,dict_f16:REAL,"
        "dict_f32:REAL,dict_f64:DOUBLE,dict_string:VARCHAR,"
        "dict_binary:VARBINARY,dict_large_string:VARCHAR,"
        "dict_large_binary:VARBINARY>");
    NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
    constexpr vector_size_t kStart = 1'020;
    constexpr vector_size_t kRows = 17;
    std::vector<VectorPtr> columns;
    for (uint32_t column = 0; column < 16; ++column) {
      columns.push_back(decoder.decodeColumn(column, kStart, kRows));
    }
    for (vector_size_t row = 0; row < kRows; ++row) {
      const auto absolute = kStart + row;
      for (const auto& column : columns) {
        EXPECT_EQ(column->isNullAt(row), absolute % 29 == 0);
      }
      if (absolute % 29 == 0) {
        continue;
      }
      const auto odd = absolute % 2 != 0;
      EXPECT_EQ(columns[0]->as<SimpleVector<bool>>()->valueAt(row), odd);
      EXPECT_EQ(
          columns[1]->as<SimpleVector<int8_t>>()->valueAt(row), odd ? 7 : -8);
      EXPECT_EQ(
          columns[2]->as<SimpleVector<int16_t>>()->valueAt(row),
          odd ? std::numeric_limits<uint8_t>::max() : 0);
      EXPECT_EQ(
          columns[3]->as<SimpleVector<int16_t>>()->valueAt(row),
          odd ? 2047 : -1024);
      EXPECT_EQ(
          columns[4]->as<SimpleVector<int32_t>>()->valueAt(row),
          odd ? std::numeric_limits<uint16_t>::max() : 0);
      EXPECT_EQ(
          columns[5]->as<SimpleVector<int32_t>>()->valueAt(row),
          odd ? 200'007 : -100'003);
      EXPECT_EQ(
          columns[6]->as<SimpleVector<int64_t>>()->valueAt(row),
          odd ? std::numeric_limits<uint32_t>::max() : 0);
      EXPECT_EQ(
          columns[7]->as<SimpleVector<int64_t>>()->valueAt(row),
          odd ? 2'000'000'007 : -1'000'000'003);
      EXPECT_EQ(
          columns[8]->as<SimpleVector<int128_t>>()->valueAt(row),
          odd ? static_cast<int128_t>(std::numeric_limits<uint64_t>::max())
              : 0);
      EXPECT_FLOAT_EQ(
          columns[9]->as<SimpleVector<float>>()->valueAt(row),
          odd ? 2.25 : -1.5);
      EXPECT_FLOAT_EQ(
          columns[10]->as<SimpleVector<float>>()->valueAt(row),
          odd ? 2.25 : -1.5);
      EXPECT_DOUBLE_EQ(
          columns[11]->as<SimpleVector<double>>()->valueAt(row),
          odd ? 2.25 : -1.5);
      EXPECT_EQ(
          columns[12]->as<SimpleVector<StringView>>()->valueAt(row).str(),
          odd ? "beta" : "alpha");
      EXPECT_EQ(
          columns[13]->as<SimpleVector<StringView>>()->valueAt(row).str(),
          odd ? "bbb" : "aa");
      EXPECT_EQ(
          columns[14]->as<SimpleVector<StringView>>()->valueAt(row).str(),
          odd ? "large-b" : "large-a");
      EXPECT_EQ(
          columns[15]->as<SimpleVector<StringView>>()->valueAt(row).str(),
          odd ? "large-bbb" : "large-aa");
    }
  }
}

TEST_F(NativeLanceTest, decodesColonBearingDictionaryValueTypes) {
  for (const auto* fileName :
       {"dictionary_logical_values_v2_0.lance",
        "dictionary_logical_values_v2_1.lance",
        "dictionary_logical_values_v2_2.lance"}) {
    auto file = load(fileName);
    EXPECT_EQ(file.metadata->rowType()->size(), 17);
    NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
    constexpr vector_size_t kStart = 1'020;
    constexpr vector_size_t kRows = 17;
    std::vector<VectorPtr> columns;
    for (uint32_t column = 0; column < 17; ++column) {
      columns.push_back(decoder.decodeColumn(column, kStart, kRows));
    }
    for (vector_size_t row = 0; row < kRows; ++row) {
      const auto absolute = kStart + row;
      for (const auto& column : columns) {
        EXPECT_EQ(column->isNullAt(row), absolute % 29 == 0);
      }
      if (absolute % 29 == 0) {
        continue;
      }
      const auto odd = absolute % 2 != 0;
      EXPECT_EQ(
          columns[0]->as<SimpleVector<int32_t>>()->valueAt(row), odd ? 11 : -7);
      EXPECT_EQ(
          columns[1]->as<SimpleVector<int32_t>>()->valueAt(row), odd ? 11 : -7);
      EXPECT_EQ(
          columns[2]->as<SimpleVector<int64_t>>()->valueAt(row), odd ? 11 : 7);
      EXPECT_EQ(
          columns[3]->as<SimpleVector<int64_t>>()->valueAt(row),
          odd ? 11'003 : 7'001);
      EXPECT_EQ(
          columns[4]->as<SimpleVector<int64_t>>()->valueAt(row),
          odd ? 11'000'003 : 7'000'001);
      EXPECT_EQ(
          columns[5]->as<SimpleVector<int64_t>>()->valueAt(row),
          odd ? 11'000'000'003 : 7'000'000'001);
      EXPECT_EQ(
          columns[6]->as<SimpleVector<Timestamp>>()->valueAt(row),
          Timestamp(odd ? 11 : -7, 0));
      EXPECT_EQ(
          columns[7]->as<SimpleVector<Timestamp>>()->valueAt(row),
          Timestamp::fromMillis(odd ? 11'003 : -7'001));
      EXPECT_EQ(
          columns[8]->as<SimpleVector<Timestamp>>()->valueAt(row),
          Timestamp::fromMicros(odd ? 11'000'003 : -7'000'001));
      EXPECT_EQ(
          columns[9]->as<SimpleVector<Timestamp>>()->valueAt(row),
          Timestamp::fromNanos(odd ? 11'000'000'003 : -7'000'000'001));
      EXPECT_EQ(
          columns[10]->as<SimpleVector<int64_t>>()->valueAt(row),
          (odd ? 11 : -7) * 1'000);
      EXPECT_EQ(
          columns[11]->as<SimpleVector<int64_t>>()->valueAt(row),
          odd ? 11'003 : -7'001);
      EXPECT_EQ(
          columns[12]->as<SimpleVector<int64_t>>()->valueAt(row),
          odd ? 11 : -7);
      EXPECT_EQ(
          columns[13]->as<SimpleVector<int64_t>>()->valueAt(row),
          odd ? 11 : -7);
      EXPECT_EQ(
          columns[14]->as<SimpleVector<int64_t>>()->valueAt(row),
          odd ? 1'103 : -701);
      EXPECT_EQ(
          columns[15]->as<SimpleVector<int128_t>>()->valueAt(row),
          odd ? static_cast<int128_t>(11'000'000'000'000'000'000ULL) + 3
              : -static_cast<int128_t>(7'000'000'000'000'000'000ULL) - 1);
      const auto fixed =
          columns[16]->as<SimpleVector<StringView>>()->valueAt(row);
      ASSERT_EQ(fixed.size(), 3);
      EXPECT_EQ(static_cast<uint8_t>(fixed.data()[0]), odd ? 0xfe : 0x01);
    }
  }
}

TEST_F(NativeLanceTest, decodesConstantAndEmptyStructFixture) {
  auto file = load("constant_and_empty_v2_2.lance");
  EXPECT_EQ(
      file.metadata->rowType()->toString(),
      "ROW<constant_i32:INTEGER,constant_string:VARCHAR,"
      "constant_list:ARRAY<INTEGER>,empty_struct:ROW<>,"
      "all_null_fsl:ARRAY<INTEGER>>");
  NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
  constexpr vector_size_t kStart = 1'020;
  constexpr vector_size_t kRows = 17;
  const auto integers = decoder.decodeColumn(0, kStart, kRows);
  const auto strings = decoder.decodeColumn(1, kStart, kRows);
  const auto lists = decoder.decodeColumn(2, kStart, kRows);
  const auto emptyStructs = decoder.decodeColumn(3, kStart, kRows);
  const auto allNullFixed = decoder.decodeColumn(4, kStart, kRows);
  const auto* arrays = lists->as<ArrayVector>();
  const auto* fixedArrays = allNullFixed->as<ArrayVector>();
  ASSERT_NE(arrays, nullptr);
  ASSERT_NE(fixedArrays, nullptr);
  ASSERT_NE(emptyStructs->as<RowVector>(), nullptr);
  EXPECT_EQ(emptyStructs->as<RowVector>()->childrenSize(), 0);
  for (vector_size_t row = 0; row < kRows; ++row) {
    const auto absolute = kStart + row;
    EXPECT_EQ(integers->isNullAt(row), absolute % 17 == 0);
    if (!integers->isNullAt(row)) {
      EXPECT_EQ(integers->asFlatVector<int32_t>()->valueAt(row), 7);
    }
    EXPECT_EQ(strings->isNullAt(row), absolute % 19 == 0);
    if (!strings->isNullAt(row)) {
      EXPECT_EQ(
          strings->asFlatVector<StringView>()->valueAt(row).str(),
          "constant-string-value");
    }
    EXPECT_EQ(lists->isNullAt(row), absolute % 4 == 0);
    if (!lists->isNullAt(row)) {
      const auto expectedSize = absolute % 4 == 1 ? 0
          : absolute % 4 == 2                     ? 3
                                                  : 1;
      EXPECT_EQ(arrays->sizeAt(row), expectedSize);
      for (vector_size_t item = 0; item < arrays->sizeAt(row); ++item) {
        const auto index = arrays->offsetAt(row) + item;
        EXPECT_EQ(
            arrays->elements()->isNullAt(index),
            absolute % 4 == 2 && item == 1);
        if (!arrays->elements()->isNullAt(index)) {
          EXPECT_EQ(
              arrays->elements()->asFlatVector<int32_t>()->valueAt(index), 7);
        }
      }
    }
    EXPECT_FALSE(emptyStructs->isNullAt(row));
    EXPECT_EQ(allNullFixed->isNullAt(row), absolute % 23 == 0);
    if (!allNullFixed->isNullAt(row)) {
      EXPECT_EQ(fixedArrays->sizeAt(row), 3);
      for (vector_size_t item = 0; item < 3; ++item) {
        EXPECT_TRUE(fixedArrays->elements()->isNullAt(
            fixedArrays->offsetAt(row) + item));
      }
    }
  }
}

TEST_F(NativeLanceTest, decodesFullZipPerValueGeneralFixture) {
  auto file = load("fullzip_general_v2_2.lance");
  NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
  constexpr vector_size_t kStart = 1'020;
  constexpr vector_size_t kRows = 17;
  const auto decoded = decoder.decodeColumn(0, kStart, kRows);
  std::string prefix;
  for (int32_t i = 0; i < 2'048; ++i) {
    prefix.append("compressible-fullzip-general-");
  }
  for (vector_size_t row = 0; row < kRows; ++row) {
    const auto absolute = kStart + row;
    EXPECT_EQ(decoded->isNullAt(row), absolute % 17 == 0);
    if (!decoded->isNullAt(row)) {
      EXPECT_EQ(
          decoded->asFlatVector<StringView>()->valueAt(row).str(),
          fmt::format("{}-{}", prefix, absolute));
    }
  }
}

TEST_F(NativeLanceTest, rejectsUnrepresentableStructuralScalarTypes) {
  const auto expectRejected = [&](const char* fileName,
                                  const char* expectedMessage,
                                  bool failsInMetadata) {
    EXPECT_THROW(
        {
          try {
            auto file = load(fileName);
            if (!failsInMetadata) {
              NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
              decoder.decodeColumn(0, 0, 1);
            }
          } catch (const BoltException& error) {
            EXPECT_NE(error.message().find(expectedMessage), std::string::npos);
            throw;
          }
        },
        BoltException);
  };

  expectRejected(
      "reject_decimal256_v2_2.lance",
      "Decimal256 cannot be represented losslessly",
      true);
  expectRejected(
      "reject_negative_scale_v2_2.lance",
      "Negative Lance decimal scales are not supported",
      true);
  expectRejected(
      "reject_lossy_duration_v2_2.lance",
      "cannot be represented losslessly in Bolt milliseconds",
      false);
  expectRejected(
      "reject_unaligned_date64_v2_2.lance",
      "not aligned to a whole day",
      false);
}

TEST_F(NativeLanceTest, decodesCanonicalV2MultiPageBinary) {
  const auto file = load("v2_0_self_described.lance");
  EXPECT_EQ(file.metadata->footer().majorVersion, 2);
  EXPECT_EQ(file.metadata->footer().minorVersion, 0);
  EXPECT_EQ(file.metadata->numRows(), 257);
  EXPECT_EQ(
      file.metadata->rowType()->toString(), "ROW<id:INTEGER,name:VARCHAR>");
  EXPECT_EQ(file.metadata->columns().size(), 2);
  EXPECT_EQ(file.metadata->physicalColumnIndex(0), 0);
  EXPECT_EQ(file.metadata->physicalColumnIndex(1), 1);

  const NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
  const auto ids = decoder.decodeColumn(0, 15, 5);
  const auto* rawIds = ids->asFlatVector<int32_t>()->rawValues();
  for (vector_size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(rawIds[i], 15 + i);
  }

  const auto names = decoder.decodeColumn(1, 13, 5);
  const auto* strings = names->asFlatVector<StringView>();
  EXPECT_EQ(strings->valueAt(0).str(), "value-0013-deterministic-fixture");
  EXPECT_TRUE(names->isNullAt(1));
  EXPECT_EQ(strings->valueAt(2).str(), "value-0015-deterministic-fixture");
  EXPECT_EQ(strings->valueAt(3).str(), "value-0016-deterministic-fixture");
  EXPECT_EQ(strings->valueAt(4).str(), "value-0017-deterministic-fixture");
}

TEST_F(NativeLanceTest, rowReaderDecodesMixedFixedAndVariableColumns) {
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(
      openFile("v2_0_self_described.lance", *pool_), readerOptions);
  auto rowReader = reader.createRowReader({});

  VectorPtr result;
  EXPECT_EQ(rowReader->next(20, result), 20);
  const auto* rows = result->as<RowVector>();
  ASSERT_NE(rows, nullptr);
  EXPECT_EQ(rows->childAt(0)->asFlatVector<int32_t>()->valueAt(13), 13);
  EXPECT_EQ(
      rows->childAt(1)->asFlatVector<StringView>()->valueAt(13).str(),
      "value-0013-deterministic-fixture");
  EXPECT_TRUE(rows->childAt(1)->isNullAt(14));
}

TEST_F(NativeLanceTest, byteRangeOwnsDisjointPageRows) {
  const auto file = load("v2_0_self_described.lance");
  const auto& pages = file.metadata->columns()[0].pages();
  ASSERT_GE(pages.size(), 2);
  ASSERT_GT(pages[0].buffer_offsets_size(), 0);
  ASSERT_GT(pages[1].buffer_offsets_size(), 0);
  const auto firstOffset = *std::min_element(
      pages[0].buffer_offsets().begin(), pages[0].buffer_offsets().end());
  const auto secondOffset = *std::min_element(
      pages[1].buffer_offsets().begin(), pages[1].buffer_offsets().end());
  ASSERT_LT(firstOffset, secondOffset);

  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader firstReader(
      openFile("v2_0_self_described.lance", *pool_), readerOptions);
  dwio::common::RowReaderOptions firstOptions;
  firstOptions.range(firstOffset, secondOffset - firstOffset);
  auto first = firstReader.createRowReader(firstOptions);
  EXPECT_EQ(first->nextRowNumber(), 0);
  VectorPtr firstResult;
  EXPECT_EQ(first->next(1'000, firstResult), pages[0].length());
  EXPECT_EQ(firstResult->size(), pages[0].length());
  EXPECT_EQ(first->next(1, firstResult), 0);

  NativeLanceReader secondReader(
      openFile("v2_0_self_described.lance", *pool_), readerOptions);
  dwio::common::RowReaderOptions secondOptions;
  secondOptions.range(
      secondOffset, std::numeric_limits<uint64_t>::max() - secondOffset);
  auto second = secondReader.createRowReader(secondOptions);
  EXPECT_EQ(second->nextRowNumber(), pages[0].length());
  VectorPtr secondResult;
  EXPECT_EQ(
      second->next(1'000, secondResult),
      file.metadata->numRows() - pages[0].length());
  EXPECT_EQ(secondResult->size(), file.metadata->numRows() - pages[0].length());
  EXPECT_EQ(second->next(1, secondResult), 0);
}

TEST_F(NativeLanceTest, structChildCanAnchorFileSplits) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeStructSplitAnchorFile()), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(
      metadata.rowRangesForFileRange(0, 64),
      (std::vector<std::pair<uint64_t, uint64_t>>{{0, 2}}));
  EXPECT_EQ(
      metadata.rowRangesForFileRange(64, 128),
      (std::vector<std::pair<uint64_t, uint64_t>>{{2, 4}}));

  NativeLanceDecoder decoder(*input, metadata, *pool_);
  const auto decoded = decoder.decodeColumn(0, 1, 2);
  const auto* rows = decoded->as<RowVector>();
  ASSERT_NE(rows, nullptr);
  const auto* values = rows->childAt(0)->asFlatVector<int32_t>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0), 2);
  EXPECT_EQ(values->valueAt(1), 3);
}

TEST_F(NativeLanceTest, emptyPackedStructUsesOnePhysicalColumn) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeEmptyPackedStructFile()), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(metadata.numRows(), 0);
  EXPECT_EQ(metadata.columns().size(), 1);
  EXPECT_EQ(metadata.physicalColumnSpan(0), 1);
  NativeLanceDecoder decoder(*input, metadata, *pool_);
  EXPECT_EQ(decoder.decodeColumn(0, 0, 0)->size(), 0);
}

TEST_F(NativeLanceTest, rowReaderBatchProjectionAndSkip) {
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(openFile("sample.lance", *pool_), readerOptions);
  EXPECT_EQ(reader.numberOfRows(), 20);
  EXPECT_EQ(reader.typeWithId()->type()->toString(), "ROW<a:BIGINT,b:DOUBLE>");

  dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.select(std::make_shared<dwio::common::ColumnSelector>(
      reader.rowType(), std::vector<std::string>{"b"}));
  auto rowReader = reader.createRowReader(rowReaderOptions);
  EXPECT_EQ(rowReader->nextRowNumber(), 0);
  EXPECT_EQ(rowReader->nextReadSize(4), 4);
  EXPECT_EQ(rowReader->skip(3), 3);
  EXPECT_EQ(rowReader->nextRowNumber(), 3);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(4, result), 4);
  ASSERT_EQ(result->type()->toString(), "ROW<b:DOUBLE>");
  ASSERT_EQ(result->size(), 4);
  const auto* values =
      result->as<RowVector>()->childAt(0)->asFlatVector<double>();
  for (vector_size_t i = 0; i < 4; ++i) {
    EXPECT_DOUBLE_EQ(values->valueAt(i), i + 4);
  }
  EXPECT_EQ(rowReader->skip(100), 13);
  EXPECT_EQ(rowReader->nextRowNumber(), dwio::common::RowReader::kAtEnd);
  EXPECT_EQ(rowReader->nextReadSize(1), dwio::common::RowReader::kAtEnd);
  EXPECT_EQ(rowReader->next(1, result), 0);
}

TEST_F(NativeLanceTest, rowReaderAppliesScanSpecFilter) {
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(openFile("sample.lance", *pool_), readerOptions);

  auto scanSpec = std::make_shared<common::ScanSpec>("<root>");
  scanSpec->addField("a", 0)->setFilter(
      std::make_unique<common::BigintRange>(8, 10, false));
  scanSpec->addField("b", 1);
  dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.setScanSpec(scanSpec);
  auto rowReader = reader.createRowReader(rowReaderOptions);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(20, result), 20);
  ASSERT_EQ(result->size(), 3);
  const auto* integers =
      result->as<RowVector>()->childAt(0)->as<SimpleVector<int64_t>>();
  const auto* doubles =
      result->as<RowVector>()->childAt(1)->as<SimpleVector<double>>();
  for (vector_size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(integers->valueAt(i), i + 8);
    EXPECT_DOUBLE_EQ(doubles->valueAt(i), i + 8);
  }
}

TEST_F(NativeLanceTest, rowReaderAppliesNestedStructFilter) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makePackedStructFile()), *pool_);
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(std::move(input), readerOptions);

  auto scanSpec = std::make_shared<common::ScanSpec>("<root>");
  auto* packed =
      scanSpec->addFieldRecursively("packed", *reader.rowType()->childAt(0), 0);
  packed->childByName("x")->setFilter(
      std::make_unique<common::BigintRange>(101, 102, false));
  dwio::common::RowReaderOptions options;
  options.setScanSpec(scanSpec);
  auto rowReader = reader.createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(4, result), 4);
  ASSERT_EQ(result->size(), 2);
  const auto& packedValues = result->as<RowVector>()->childAt(0);
  const auto* rows = packedValues->wrappedVector()->as<RowVector>();
  ASSERT_NE(rows, nullptr);
  const auto* x = rows->childAt(0)->asFlatVector<int64_t>();
  const auto* y = rows->childAt(1)->asFlatVector<int32_t>();
  ASSERT_NE(x, nullptr);
  ASSERT_NE(y, nullptr);
  EXPECT_EQ(x->valueAt(packedValues->wrappedIndex(0)), 101);
  EXPECT_EQ(x->valueAt(packedValues->wrappedIndex(1)), 102);
  EXPECT_EQ(y->valueAt(packedValues->wrappedIndex(0)), 201);
  EXPECT_EQ(y->valueAt(packedValues->wrappedIndex(1)), 202);
}

TEST_F(NativeLanceTest, filterOnlyNestedStructFieldFiltersRows) {
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(
      openFile("packed_fixed_v2_2.lance", *pool_), readerOptions);

  auto scanSpec = std::make_shared<common::ScanSpec>("<root>");
  auto* packed = scanSpec->getOrCreateChild("packed");
  packed->getOrCreateChild("x")->setFilter(
      std::make_unique<common::BigintRange>(1, 2, false));
  dwio::common::RowReaderOptions options;
  options.setScanSpec(scanSpec);
  auto rowReader = reader.createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(20, result), 20);
  EXPECT_EQ(result->type()->toString(), "ROW<>");
  EXPECT_EQ(result->size(), 2);
}

TEST_F(NativeLanceTest, rowReaderAppliesNullableStructFilter) {
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(
      openFile("packed_fixed_v2_2.lance", *pool_), readerOptions);

  auto scanSpec = std::make_shared<common::ScanSpec>("<root>");
  auto* packed =
      scanSpec->addFieldRecursively("packed", *reader.rowType()->childAt(0), 0);
  packed->setFilter(std::make_unique<common::IsNull>());
  dwio::common::RowReaderOptions options;
  options.setScanSpec(scanSpec);
  auto rowReader = reader.createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(20, result), 20);
  ASSERT_EQ(result->size(), 2);
  const auto& packedValues = result->as<RowVector>()->childAt(0);
  EXPECT_TRUE(packedValues->isNullAt(0));
  EXPECT_TRUE(packedValues->isNullAt(1));
}

TEST_F(NativeLanceTest, rowReaderAppliesMutationDeletionVector) {
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(openFile("sample.lance", *pool_), readerOptions);
  auto rowReader = reader.createRowReader();

  std::array<uint64_t, 1> deletedRows{0};
  bits::setBit(deletedRows.data(), 0);
  bits::setBit(deletedRows.data(), 3);
  bits::setBit(deletedRows.data(), 19);
  dwio::common::Mutation mutation{.deletedRows = deletedRows.data()};
  VectorPtr result;
  EXPECT_EQ(rowReader->next(20, result, &mutation), 20);
  ASSERT_EQ(result->size(), 17);
  const auto* values =
      result->as<RowVector>()->childAt(0)->as<SimpleVector<int64_t>>();
  ASSERT_NE(values, nullptr);
  for (vector_size_t row = 0; row < result->size(); ++row) {
    const auto expected = row < 2 ? row + 2 : row + 3;
    EXPECT_EQ(values->valueAt(row), expected);
  }

  auto batched = reader.createRowReader();
  std::array<uint64_t, 1> oddRows{0xAAAAAAAAAAAAAAAAULL};
  dwio::common::Mutation oddMutation{.deletedRows = oddRows.data()};
  uint64_t scanned = 0;
  while (const auto batchScanned = batched->next(10, result, &oddMutation)) {
    EXPECT_EQ(batchScanned, 10);
    ASSERT_EQ(result->size(), 5);
    const auto* batchValues =
        result->as<RowVector>()->childAt(0)->as<SimpleVector<int64_t>>();
    for (vector_size_t row = 0; row < result->size(); ++row) {
      EXPECT_EQ(batchValues->valueAt(row), scanned + row * 2 + 1);
    }
    scanned += batchScanned;
  }
  EXPECT_EQ(scanned, 20);

  auto allDeleted = reader.createRowReader();
  std::array<uint64_t, 1> allRows{std::numeric_limits<uint64_t>::max()};
  dwio::common::Mutation allMutation{.deletedRows = allRows.data()};
  EXPECT_EQ(allDeleted->next(20, result, &allMutation), 20);
  EXPECT_EQ(result->size(), 0);
}

TEST_F(NativeLanceTest, selectiveReaderCombinesFilterAndMutation) {
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(openFile("sample.lance", *pool_), readerOptions);
  auto scanSpec = std::make_shared<common::ScanSpec>("<root>");
  scanSpec->addField("a", 0)->setFilter(
      std::make_unique<common::BigintRange>(8, 12, false));
  scanSpec->addField("b", 1);
  dwio::common::RowReaderOptions options;
  options.setScanSpec(scanSpec);
  auto rowReader = reader.createRowReader(options);

  std::array<uint64_t, 1> deletedRows{0};
  bits::setBit(deletedRows.data(), 7);
  bits::setBit(deletedRows.data(), 9);
  dwio::common::Mutation mutation{.deletedRows = deletedRows.data()};
  VectorPtr result;
  EXPECT_EQ(rowReader->next(20, result, &mutation), 20);
  ASSERT_EQ(result->size(), 3);
  const auto* integers =
      result->as<RowVector>()->childAt(0)->as<SimpleVector<int64_t>>();
  EXPECT_EQ(integers->valueAt(0), 9);
  EXPECT_EQ(integers->valueAt(1), 11);
  EXPECT_EQ(integers->valueAt(2), 12);
}

TEST_F(NativeLanceTest, reportsOnDiskColumnStatistics) {
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(openFile("sample.lance", *pool_), readerOptions);
  const auto root = reader.columnStatistics(reader.typeWithId()->id());
  ASSERT_NE(root, nullptr);
  EXPECT_EQ(root->getSize(), 2 * 20 * sizeof(int64_t));
  EXPECT_FALSE(root->getNumberOfValues().has_value());
  EXPECT_FALSE(root->hasNull().has_value());

  const auto first =
      reader.columnStatistics(reader.typeWithId()->childAt(0)->id());
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(first->getSize(), 20 * sizeof(int64_t));
  EXPECT_EQ(reader.columnStatistics(999), nullptr);
}

TEST_F(NativeLanceTest, selectiveReaderLateMaterializesProjectedColumn) {
  auto readFile = std::make_shared<LocalReadFile>("examples/sample.lance");
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(std::move(input), readerOptions);
  readFile->resetBytesRead();

  auto scanSpec = std::make_shared<common::ScanSpec>("<root>");
  auto* filter = scanSpec->getOrCreateChild("a");
  filter->setFilter(common::createBigintValues({2, 5, 9}, false));
  filter->setProjectOut(false);
  scanSpec->addField("b", 0);
  dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.setScanSpec(scanSpec);
  auto rowReader = reader.createRowReader(rowReaderOptions);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(20, result), 20);
  ASSERT_EQ(result->type()->toString(), "ROW<b:DOUBLE>");
  ASSERT_EQ(result->size(), 3);
  const auto* values =
      result->as<RowVector>()->childAt(0)->asFlatVector<double>();
  EXPECT_DOUBLE_EQ(values->valueAt(0), 2);
  EXPECT_DOUBLE_EQ(values->valueAt(1), 5);
  EXPECT_DOUBLE_EQ(values->valueAt(2), 9);
  EXPECT_EQ(readFile->bytesRead(), 20 * sizeof(int64_t) + 3 * sizeof(double));
}

TEST_F(NativeLanceTest, readerFactoryIsNative) {
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReaderFactory factory;
  auto reader =
      factory.createReader(openFile("sample.lance", *pool_), readerOptions);

  ASSERT_NE(dynamic_cast<NativeLanceReader*>(reader.get()), nullptr);
  EXPECT_EQ(reader->numberOfRows(), 20);
}

TEST_F(NativeLanceTest, parsesNestedSchemaAndPhysicalMapping) {
  const auto file = load("list_struct_v2_0.lance");
  EXPECT_EQ(
      file.metadata->rowType()->toString(),
      "ROW<id:INTEGER,data:ARRAY<ROW<c:VARCHAR,b:VARCHAR>>>");
  EXPECT_EQ(file.metadata->columns().size(), 5);
  EXPECT_EQ(file.metadata->physicalColumnIndex(0), 0);
  EXPECT_EQ(file.metadata->physicalColumnIndex(1), 1);
  EXPECT_EQ(file.metadata->physicalColumnSpan(1), 4);
  EXPECT_EQ(file.metadata->physicalColumnLogicalType(3), "string");
  EXPECT_EQ(file.metadata->physicalColumnLogicalType(4), "string");

  const NativeLanceDecoder decoder(*file.input, *file.metadata, *pool_);
  const auto data = decoder.decodeColumn(1, 0, 2);
  const auto* lists = data->as<ArrayVector>();
  ASSERT_NE(lists, nullptr) << data->toString();
  ASSERT_EQ(lists->size(), 2);
  ASSERT_EQ(lists->sizeAt(0), 1);
  ASSERT_EQ(lists->sizeAt(1), 1);
  const auto* elements = lists->elements()->as<RowVector>();
  ASSERT_NE(elements, nullptr) << lists->elements()->toString();
  const auto* c = elements->childAt(0)->asFlatVector<StringView>();
  const auto* b = elements->childAt(1)->asFlatVector<StringView>();
  ASSERT_NE(c, nullptr) << elements->childAt(0)->toString();
  ASSERT_NE(b, nullptr) << elements->childAt(1)->toString();
  EXPECT_EQ(c->valueAt(0).str(), "c3");
  EXPECT_EQ(c->valueAt(1).str(), "c4");
  EXPECT_EQ(b->valueAt(0).str(), "b3");
  EXPECT_EQ(b->valueAt(1).str(), "b4");

  const auto slicedData = decoder.decodeColumn(1, 1, 1);
  const auto* sliced = slicedData->as<ArrayVector>();
  ASSERT_NE(sliced, nullptr);
  ASSERT_EQ(sliced->size(), 1);
  ASSERT_EQ(sliced->sizeAt(0), 1);
  const auto* slicedElements = sliced->elements()->as<RowVector>();
  ASSERT_NE(slicedElements, nullptr) << sliced->elements()->toString();
  ASSERT_NE(slicedElements->childAt(0)->asFlatVector<StringView>(), nullptr)
      << slicedElements->childAt(0)->toString();
  ASSERT_NE(slicedElements->childAt(1)->asFlatVector<StringView>(), nullptr)
      << slicedElements->childAt(1)->toString();
  EXPECT_EQ(
      slicedElements->childAt(0)->asFlatVector<StringView>()->valueAt(0).str(),
      "c4");
  EXPECT_EQ(
      slicedElements->childAt(1)->asFlatVector<StringView>()->valueAt(0).str(),
      "b4");
}

TEST_F(NativeLanceTest, decodesBooleanNullsAndTimestamp) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeBooleanTimestampFile()), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto flags = decoder.decodeColumn(0, 1, 3);
  EXPECT_TRUE(flags->isNullAt(0));
  EXPECT_FALSE(flags->isNullAt(1));
  EXPECT_TRUE(flags->asFlatVector<bool>()->valueAt(1));
  EXPECT_FALSE(flags->isNullAt(2));
  EXPECT_FALSE(flags->asFlatVector<bool>()->valueAt(2));

  const auto timestamps = decoder.decodeColumn(1, 0, 5);
  const auto* values = timestamps->asFlatVector<Timestamp>();
  EXPECT_EQ(values->valueAt(0), Timestamp::fromMicros(-1));
  EXPECT_EQ(values->valueAt(1), Timestamp::fromMicros(0));
  EXPECT_EQ(values->valueAt(2), Timestamp::fromMicros(1));
  EXPECT_EQ(values->valueAt(3), Timestamp::fromMicros(1'000'001));
  EXPECT_EQ(values->valueAt(4), Timestamp::fromMicros(2'000'002));

  const auto allNulls = decoder.decodeColumn(2, 1, 3);
  EXPECT_EQ(allNulls->size(), 3);
  EXPECT_TRUE(allNulls->isNullAt(0));
  EXPECT_TRUE(allNulls->isNullAt(1));
  EXPECT_TRUE(allNulls->isNullAt(2));
}

TEST_F(NativeLanceTest, decodesExtendedV20PrimitiveTypes) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeExtendedPrimitiveFile()), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(
      metadata.rowType()->toString(),
      "ROW<u64:HUGEINT,f16:REAL,date64:DATE,time_s:LANCE_time32:s,"
      "time_ms:LANCE_time32:ms,time_us:LANCE_time64:us,"
      "time_ns:LANCE_time64:ns,duration_s:INTERVAL DAY TO SECOND,"
      "duration_us:INTERVAL DAY TO SECOND,duration_ns:INTERVAL DAY TO SECOND>");
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto u64 = decoder.decodeColumn(0, 0, 2)->asFlatVector<int128_t>();
  EXPECT_EQ(u64->valueAt(0), 0);
  EXPECT_EQ(
      u64->valueAt(1),
      static_cast<int128_t>(std::numeric_limits<uint64_t>::max()));
  const auto f16 = decoder.decodeColumn(1, 0, 2)->asFlatVector<float>();
  EXPECT_FLOAT_EQ(f16->valueAt(0), 1.0);
  EXPECT_FLOAT_EQ(f16->valueAt(1), -2.0);
  const auto dates = decoder.decodeColumn(2, 0, 2)->asFlatVector<int32_t>();
  EXPECT_EQ(dates->valueAt(0), -1);
  EXPECT_EQ(dates->valueAt(1), 2);

  const std::array<std::array<int64_t, 2>, 4> expectedTimes{
      std::array<int64_t, 2>{1, 86'399},
      std::array<int64_t, 2>{1, 86'399'999},
      std::array<int64_t, 2>{1, 86'399'999'999},
      std::array<int64_t, 2>{1, 86'399'999'999'999}};
  for (uint32_t column = 3; column <= 6; ++column) {
    const auto values =
        decoder.decodeColumn(column, 0, 2)->asFlatVector<int64_t>();
    EXPECT_EQ(values->valueAt(0), expectedTimes[column - 3][0]);
    EXPECT_EQ(values->valueAt(1), expectedTimes[column - 3][1]);
  }

  for (uint32_t column = 7; column <= 9; ++column) {
    const auto values =
        decoder.decodeColumn(column, 0, 2)->asFlatVector<int64_t>();
    EXPECT_EQ(values->valueAt(0), -2'000);
    EXPECT_EQ(values->valueAt(1), 3'000);
  }
}

TEST_F(NativeLanceTest, decodesSignedAndUnsignedBitpackedRanges) {
  auto readFile = std::make_shared<InMemoryReadFile>(makeBitpackedFile());
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);
  readFile->resetBytesRead();

  const auto signedValues = decoder.decodeColumn(0, 1, 4);
  const auto* rawSigned = signedValues->asFlatVector<int16_t>();
  EXPECT_EQ(rawSigned->valueAt(0), -3);
  EXPECT_EQ(rawSigned->valueAt(1), -1);
  EXPECT_EQ(rawSigned->valueAt(2), 0);
  EXPECT_EQ(rawSigned->valueAt(3), 7);

  const auto unsignedValues = decoder.decodeColumn(1, 1, 4);
  const auto* rawUnsigned = unsignedValues->asFlatVector<int32_t>();
  EXPECT_EQ(rawUnsigned->valueAt(0), 1);
  EXPECT_EQ(rawUnsigned->valueAt(1), 7);
  EXPECT_EQ(rawUnsigned->valueAt(2), 16);
  EXPECT_EQ(rawUnsigned->valueAt(3), 31);
  EXPECT_EQ(readFile->bytesRead(), 8);
}

TEST_F(NativeLanceTest, decodesBitpackedForNonNegativeAcrossChunks) {
  auto readFile =
      std::make_shared<InMemoryReadFile>(makeBitpackedForNonNegFile());
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);
  constexpr uint64_t kStart = 1'020;
  constexpr uint64_t kCount = 1'030;
  constexpr std::array<uint8_t, 4> kBitWidths{5, 13, 23, 37};
  for (uint32_t column = 0; column < kBitWidths.size(); ++column) {
    const auto decoded = decoder.decodeColumn(column, kStart, kCount);
    const auto mask = (uint64_t{1} << kBitWidths[column]) - 1;
    for (uint64_t row = 0; row < kCount; ++row) {
      const auto expected = ((kStart + row) * 37 + column * 11 + 3) & mask;
      switch (column) {
        case 0:
          EXPECT_EQ(decoded->asFlatVector<int16_t>()->valueAt(row), expected);
          break;
        case 1:
          EXPECT_EQ(decoded->asFlatVector<int32_t>()->valueAt(row), expected);
          break;
        case 2:
          EXPECT_EQ(decoded->asFlatVector<int64_t>()->valueAt(row), expected);
          break;
        case 3:
          EXPECT_EQ(
              decoded->asFlatVector<int128_t>()->valueAt(row),
              static_cast<int128_t>(expected));
          break;
      }
    }
  }
}

TEST_F(NativeLanceTest, decodesZeroWidthV20BitpackingEndToEnd) {
  auto readFile =
      std::make_shared<InMemoryReadFile>(makeZeroBitpackedV20File());
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(metadata.footer().majorVersion, 2);
  EXPECT_EQ(metadata.footer().minorVersion, 0);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  decoder.prefetchColumns({0, 1, 2, 3}, 1'020, 1'030);
  decoder.materializeReadPlan();
  for (uint32_t column = 0; column < 4; ++column) {
    const auto decoded = decoder.decodeColumn(column, 1'020, 1'030);
    ASSERT_EQ(decoded->size(), 1'030);
    for (vector_size_t row = 0; row < decoded->size(); ++row) {
      if (column == 0) {
        EXPECT_EQ(decoded->asFlatVector<int32_t>()->valueAt(row), 0);
      } else if (column == 1) {
        EXPECT_EQ(decoded->asFlatVector<int64_t>()->valueAt(row), 0);
      } else {
        EXPECT_EQ(decoded->as<SimpleVector<int32_t>>()->valueAt(row), 77);
      }
    }
  }
}

TEST_F(NativeLanceTest, boltBitpackDecoderMatchesScalar) {
  constexpr uint64_t kRows = 1'037;
  for (const auto [bitWidth, outputWidth] :
       std::vector<std::pair<uint8_t, uint8_t>>{
           {5, 8}, {5, 16}, {13, 16}, {23, 32}, {29, 32}, {37, 64}}) {
    const auto mask = (uint64_t{1} << bitWidth) - 1;
    std::vector<uint64_t> values(kRows);
    for (uint64_t row = 0; row < kRows; ++row) {
      values[row] = (row * 37 + 3) & mask;
    }
    for (const auto bitOffset : {uint8_t{0}, uint8_t{3}}) {
      const auto packed = packBits(values, bitWidth, bitOffset);
      const auto outputBytes = kRows * outputWidth / 8;
      std::vector<uint8_t> scalar(outputBytes);
      std::vector<uint8_t> optimized(outputBytes);
      decodeLanceBitpackedScalar(
          packed.data(),
          packed.size(),
          bitOffset,
          kRows,
          bitWidth,
          outputWidth,
          true,
          scalar.data());
      decodeLanceBitpacked(
          packed.data(),
          packed.size(),
          bitOffset,
          kRows,
          bitWidth,
          outputWidth,
          true,
          optimized.data());
      EXPECT_EQ(optimized, scalar)
          << "bitWidth=" << static_cast<int>(bitWidth)
          << " outputWidth=" << static_cast<int>(outputWidth)
          << " bitOffset=" << static_cast<int>(bitOffset);
    }
  }
}

TEST_F(NativeLanceTest, bitpackZeroWidthProducesZeroValues) {
  constexpr uint64_t kRows = 2'049;
  constexpr uint8_t kOutputBits = 16;
  std::vector<uint16_t> scalar(kRows, 0xffff);
  std::vector<uint16_t> optimized(kRows, 0xffff);
  std::vector<uint16_t> fastLanes(kRows, 0xffff);
  const std::array<uint8_t, 1> emptyInput{0};
  decodeLanceBitpackedScalar(
      emptyInput.data(),
      0,
      0,
      kRows,
      0,
      kOutputBits,
      false,
      reinterpret_cast<uint8_t*>(scalar.data()));
  decodeLanceBitpacked(
      emptyInput.data(),
      0,
      0,
      kRows,
      0,
      kOutputBits,
      false,
      reinterpret_cast<uint8_t*>(optimized.data()));
  decodeLanceBitpackedForNonNeg(
      emptyInput.data(),
      0,
      0,
      kRows,
      0,
      kOutputBits,
      reinterpret_cast<uint8_t*>(fastLanes.data()));
  EXPECT_EQ(scalar, std::vector<uint16_t>(kRows, 0));
  EXPECT_EQ(optimized, scalar);
  EXPECT_EQ(fastLanes, scalar);
}

TEST_F(NativeLanceTest, bulkBitmapCopyMatchesScalar) {
  constexpr uint64_t kBits = 1'111;
  std::array<uint8_t, (kBits + 7) / 8 + 8> source{};
  for (uint64_t i = 0; i < kBits; ++i) {
    bits::setBit(source.data(), i, i % 17 != 0 && i % 31 != 0);
  }

  for (const auto sourceOffset :
       {uint64_t{0}, uint64_t{1}, uint64_t{3}, uint64_t{7}}) {
    for (const auto targetOffset :
         {uint64_t{0}, uint64_t{1}, uint64_t{5}, uint64_t{63}}) {
      for (const auto count :
           {uint64_t{0},
            uint64_t{1},
            uint64_t{31},
            uint64_t{64},
            uint64_t{65},
            uint64_t{1'024}}) {
        std::array<uint64_t, 20> scalar;
        std::array<uint64_t, 20> bulk;
        scalar.fill(0xa5a5a5a5a5a5a5a5);
        bulk = scalar;
        copyLanceBitmapScalar(
            source.data(), sourceOffset, count, scalar.data(), targetOffset);
        copyLanceBitmap(
            source.data(), sourceOffset, count, bulk.data(), targetOffset);
        EXPECT_EQ(bulk, scalar)
            << "sourceOffset=" << sourceOffset
            << " targetOffset=" << targetOffset << " count=" << count;
      }
    }
  }
  EXPECT_FALSE(lanceBitmapIsAllSet(source.data(), 0, kBits));
  source.fill(0xff);
  EXPECT_TRUE(lanceBitmapIsAllSet(source.data(), 0, kBits));
  EXPECT_TRUE(lanceBitmapIsAllSet(source.data(), 3, kBits));
}

TEST_F(NativeLanceTest, preservesDictionaryEncodingAndNulls) {
  auto readFile = std::make_shared<InMemoryReadFile>(makeDictionaryFile());
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto colors = decoder.decodeColumn(0, 1, 4);
  EXPECT_EQ(colors->encoding(), VectorEncoding::Simple::DICTIONARY);
  const auto* values = colors->as<SimpleVector<StringView>>();
  EXPECT_EQ(values->valueAt(0).str(), "green");
  EXPECT_TRUE(values->isNullAt(1));
  EXPECT_EQ(values->valueAt(2).str(), "red");
  EXPECT_EQ(values->valueAt(3).str(), "blue");
  EXPECT_EQ(colors->valueVector()->size(), 3);
}

TEST_F(NativeLanceTest, decodesLogicalDictionaryWithZeroBasedIndices) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeLogicalDictionaryFile()), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(metadata.rowType()->toString(), "ROW<code:INTEGER>");
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto codes = decoder.decodeColumn(0, 0, 4);
  EXPECT_EQ(codes->encoding(), VectorEncoding::Simple::DICTIONARY);
  const auto* values = codes->as<SimpleVector<int32_t>>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0), 30);
  EXPECT_EQ(values->valueAt(1), 10);
  EXPECT_EQ(values->valueAt(2), 20);
  EXPECT_EQ(values->valueAt(3), 30);
}

TEST_F(NativeLanceTest, decodesFixedSizeBinaryDictionaryItems) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeFixedSizeBinaryDictionaryFile()),
      *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(metadata.rowType()->toString(), "ROW<code:VARBINARY>");
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto decoded = decoder.decodeColumn(0, 0, 4);
  EXPECT_EQ(decoded->encoding(), VectorEncoding::Simple::DICTIONARY);
  const auto* values = decoded->as<SimpleVector<StringView>>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0).str(), "ccc");
  EXPECT_EQ(values->valueAt(1).str(), "aaa");
  EXPECT_EQ(values->valueAt(2).str(), "bbb");
  EXPECT_EQ(values->valueAt(3).str(), "ccc");
}

TEST_F(NativeLanceTest, decodesFixedSizeBinaryRange) {
  auto readFile = std::make_shared<InMemoryReadFile>(makeFixedSizeBinaryFile());
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);
  readFile->resetBytesRead();

  const auto values = decoder.decodeColumn(0, 1, 2);
  const auto* strings = values->asFlatVector<StringView>();
  EXPECT_EQ(strings->valueAt(0).str(), "bbb");
  EXPECT_EQ(strings->valueAt(1).str(), "ccc");
  EXPECT_EQ(readFile->bytesRead(), 6);
}

TEST_F(NativeLanceTest, decodesFlatFixedSizeBinaryRange) {
  auto readFile =
      std::make_shared<InMemoryReadFile>(makeFlatFixedSizeBinaryFile());
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto values = decoder.decodeColumn(0, 1, 2);
  const auto* binaries = values->asFlatVector<StringView>();
  EXPECT_EQ(binaries->valueAt(0).str(), "bbb");
  EXPECT_EQ(binaries->valueAt(1).str(), "ccc");
}

TEST_F(NativeLanceTest, decodesFixedSizeListRange) {
  auto readFile = std::make_shared<InMemoryReadFile>(makeFixedSizeListFile());
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(metadata.rowType()->toString(), "ROW<coordinates:ARRAY<INTEGER>>");
  NativeLanceDecoder decoder(*input, metadata, *pool_);
  readFile->resetBytesRead();

  const auto values = decoder.decodeColumn(0, 1, 2);
  const auto* arrays = values->as<ArrayVector>();
  ASSERT_NE(arrays, nullptr);
  EXPECT_EQ(arrays->size(), 2);
  EXPECT_EQ(arrays->sizeAt(0), 3);
  EXPECT_EQ(arrays->sizeAt(1), 3);
  const auto* elements = arrays->elements()->asFlatVector<int32_t>();
  ASSERT_NE(elements, nullptr);
  for (vector_size_t i = 0; i < 6; ++i) {
    EXPECT_EQ(elements->valueAt(i), i + 4);
  }
  EXPECT_EQ(readFile->bytesRead(), 6 * sizeof(int32_t));
}

TEST_F(NativeLanceTest, decodesNestedFixedSizeListRange) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeNestedFixedSizeListFile()),
      *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(
      metadata.rowType()->toString(), "ROW<matrix:ARRAY<ARRAY<INTEGER>>>");
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto decoded = decoder.decodeColumn(0, 1, 1);
  const auto* outer = decoded->as<ArrayVector>();
  ASSERT_NE(outer, nullptr);
  ASSERT_EQ(outer->sizeAt(0), 2);
  const auto* inner = outer->elements()->as<ArrayVector>();
  ASSERT_NE(inner, nullptr);
  ASSERT_EQ(inner->size(), 2);
  EXPECT_EQ(inner->sizeAt(0), 2);
  EXPECT_EQ(inner->sizeAt(1), 2);
  const auto* values = inner->elements()->asFlatVector<int32_t>();
  ASSERT_NE(values, nullptr);
  for (vector_size_t row = 0; row < 4; ++row) {
    EXPECT_EQ(values->valueAt(row), row + 5);
  }
}

TEST_F(NativeLanceTest, decodesFixedSizeListOfFixedSizeBinary) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeFixedSizeBinaryListFile()),
      *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(metadata.rowType()->toString(), "ROW<pairs:ARRAY<VARBINARY>>");
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto decoded = decoder.decodeColumn(0, 1, 1);
  const auto* arrays = decoded->as<ArrayVector>();
  ASSERT_NE(arrays, nullptr);
  ASSERT_EQ(arrays->sizeAt(0), 2);
  const auto* values = arrays->elements()->asFlatVector<StringView>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0).str(), "ccc");
  EXPECT_EQ(values->valueAt(1).str(), "ddd");
}

TEST_F(NativeLanceTest, preservesBFloat16ExtensionStorage) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeBFloat16ListFile()), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(
      metadata.rowType()->toString(), "ROW<values:ARRAY<LANCE_bfloat16>>");
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto decoded = decoder.decodeColumn(0, 0, 2);
  const auto* arrays = decoded->as<ArrayVector>();
  ASSERT_NE(arrays, nullptr);
  const auto* values = arrays->elements()->asFlatVector<StringView>();
  ASSERT_NE(values, nullptr);
  ASSERT_EQ(values->size(), 4);
  constexpr std::array<uint16_t, 4> kExpected{0x3f80, 0xc000, 0x4040, 0x0000};
  for (vector_size_t row = 0; row < values->size(); ++row) {
    ASSERT_EQ(values->valueAt(row).size(), sizeof(uint16_t));
    EXPECT_EQ(
        folly::Endian::little(
            folly::loadUnaligned<uint16_t>(values->valueAt(row).data())),
        kExpected[row]);
  }
}

TEST_F(NativeLanceTest, decodesCompressedFlatRanges) {
  for (const auto scheme : {"zstd", "zstd_raw", "lz4"}) {
    auto readFile =
        std::make_shared<InMemoryReadFile>(makeCompressedFile(scheme));
    auto input =
        std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
    NativeLanceMetadata metadata(*input, *pool_);
    NativeLanceDecoder decoder(*input, metadata, *pool_);

    const auto values = decoder.decodeColumn(0, 2, 3);
    const auto* integers = values->asFlatVector<int32_t>();
    EXPECT_EQ(integers->valueAt(0), 3) << scheme;
    EXPECT_EQ(integers->valueAt(1), 4) << scheme;
    EXPECT_EQ(integers->valueAt(2), 5) << scheme;
  }
}

TEST_F(NativeLanceTest, decodedPageCacheIsSharedAcrossDecoders) {
  auto readFile =
      std::make_shared<InMemoryReadFile>(makeCompressedFile("zstd"));
  auto firstInput =
      std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata metadata(*firstInput, *pool_);
  auto cache = std::make_shared<NativeLanceDecodedPageCache>(1 << 20);
  NativeLanceDecoder first(
      *firstInput,
      metadata,
      *pool_,
      true,
      nullptr,
      NativeLanceReadPlan::Options{},
      cache);

  readFile->resetBytesRead();
  const auto firstValues = first.decodeColumn(0, 2, 3);
  EXPECT_EQ(firstValues->asFlatVector<int32_t>()->valueAt(0), 3);
  const auto bytesAfterFirst = readFile->bytesRead();
  EXPECT_GT(bytesAfterFirst, 0);
  EXPECT_GT(cache->sizeBytes(), 0);

  auto secondInput =
      std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata secondMetadata(*secondInput, *pool_);
  readFile->resetBytesRead();
  NativeLanceDecoder second(
      *secondInput,
      secondMetadata,
      *pool_,
      false,
      nullptr,
      NativeLanceReadPlan::Options{},
      cache);
  const auto secondValues = second.decodeColumn(0, 3, 2);
  EXPECT_EQ(secondValues->asFlatVector<int32_t>()->valueAt(0), 4);
  EXPECT_EQ(secondValues->asFlatVector<int32_t>()->valueAt(1), 5);
  EXPECT_EQ(readFile->bytesRead(), 0);
}

TEST_F(NativeLanceTest, compressedNestedItemsReturnCopyOnWriteViews) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeCompressedListFile()), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  auto first = decoder.decodeColumn(0, 1, 2);
  const auto second = decoder.decodeColumn(0, 2, 2);
  auto* firstArray = first->as<ArrayVector>();
  const auto* secondArray = second->as<ArrayVector>();
  ASSERT_NE(firstArray, nullptr);
  ASSERT_NE(secondArray, nullptr);
  auto* firstItems = firstArray->elements()->asFlatVector<int32_t>();
  const auto* secondItems = secondArray->elements()->asFlatVector<int32_t>();
  ASSERT_EQ(firstItems->rawValues(), secondItems->rawValues());
  EXPECT_EQ(secondItems->valueAt(0), 30);

  firstItems->mutableRawValues()[0] = 100;
  EXPECT_EQ(firstItems->valueAt(0), 100);
  EXPECT_EQ(secondItems->valueAt(0), 30);
  const auto third = decoder.decodeColumn(0, 2, 2);
  EXPECT_EQ(
      third->as<ArrayVector>()->elements()->asFlatVector<int32_t>()->valueAt(0),
      30);
}

TEST_F(NativeLanceTest, decodedPageCacheCoalescesConcurrentLoads) {
  auto cache = std::make_shared<NativeLanceDecodedPageCache>(1 << 20);
  constexpr NativeLanceDecodedPageCache::Key kKey{3, 7};
  folly::Baton<> loaderEntered;
  folly::Baton<> releaseLoader;
  std::atomic<uint32_t> loadCalls{0};
  const auto load = [&]() -> VectorPtr {
    ++loadCalls;
    loaderEntered.post();
    releaseLoader.wait();
    return BaseVector::create(INTEGER(), 1, pool_.get());
  };

  auto first = std::async(
      std::launch::async, [&] { return cache->getOrLoad(kKey, load); });
  loaderEntered.wait();
  auto second = std::async(
      std::launch::async, [&] { return cache->getOrLoad(kKey, load); });
  releaseLoader.post();

  const auto firstVector = first.get();
  const auto secondVector = second.get();
  EXPECT_EQ(loadCalls, 1);
  EXPECT_EQ(firstVector.get(), secondVector.get());
}

TEST_F(NativeLanceTest, decompressedCacheCanGrowWithFileSize) {
  EnvVarGuard guard("BOLT_LANCE_DECOMPRESSED_CACHE_BYTES", std::nullopt);
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeCompressedFile("zstd")), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  decoder.decodeColumn(0, 2, 3);
  auto stats = metadata.debugStats();
  EXPECT_EQ(stats.decompressedCacheMisses, 1);
  EXPECT_EQ(stats.decompressedCacheHits, 0);

  decoder.decodeColumn(0, 3, 2);
  stats = metadata.debugStats();
  EXPECT_EQ(stats.decompressedCacheMisses, 1);
  EXPECT_GE(stats.decompressedCacheHits, 1);
}

TEST_F(NativeLanceTest, explicitDecompressedCacheLimitStillApplies) {
  EnvVarGuard guard("BOLT_LANCE_DECOMPRESSED_CACHE_BYTES", "1");
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeCompressedFile("zstd")), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  decoder.decodeColumn(0, 2, 3);
  auto stats = metadata.debugStats();
  EXPECT_EQ(stats.decompressedCacheMisses, 1);

  decoder.decodeColumn(0, 3, 2);
  stats = metadata.debugStats();
  EXPECT_EQ(stats.decompressedCacheMisses, 2);
  EXPECT_EQ(stats.decompressedCacheHits, 0);
}

TEST_F(NativeLanceTest, decodesCompressedListOffsets) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeCompressedListFile()), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto decoded = decoder.decodeColumn(0, 1, 2);
  const auto* arrays = decoded->as<ArrayVector>();
  ASSERT_NE(arrays, nullptr);
  EXPECT_EQ(arrays->sizeAt(0), 0);
  EXPECT_EQ(arrays->sizeAt(1), 3);
  const auto* values = arrays->elements()->asFlatVector<int32_t>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0), 30);
  EXPECT_EQ(values->valueAt(1), 40);
  EXPECT_EQ(values->valueAt(2), 50);
}

TEST_F(NativeLanceTest, decodesBitpackedListOffsetsAcrossChunks) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeBitpackedListOffsetsFile()),
      *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  constexpr vector_size_t kStart = 1'020;
  constexpr vector_size_t kRows = 17;
  const auto decoded = decoder.decodeColumn(0, kStart, kRows);
  const auto* arrays = decoded->as<ArrayVector>();
  ASSERT_NE(arrays, nullptr);
  const auto* values = arrays->elements()->asFlatVector<int32_t>();
  ASSERT_NE(values, nullptr);
  for (vector_size_t row = 0; row < kRows; ++row) {
    const auto absolute = kStart + row;
    EXPECT_EQ(arrays->sizeAt(row), absolute % 4);
    for (vector_size_t item = 0; item < arrays->sizeAt(row); ++item) {
      EXPECT_EQ(
          values->valueAt(arrays->offsetAt(row) + item), absolute * 10 + item);
    }
  }
}

TEST_F(NativeLanceTest, decodesBitpackedLegacyBinaryOffsetsAcrossChunks) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(
          makeBitpackedLegacyBinaryOffsetsFile()),
      *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  constexpr vector_size_t kStart = 1'020;
  constexpr vector_size_t kRows = 17;
  const auto decoded = decoder.decodeColumn(0, kStart, kRows);
  const auto* strings = decoded->asFlatVector<StringView>();
  ASSERT_NE(strings, nullptr);
  for (vector_size_t row = 0; row < kRows; ++row) {
    const auto absolute = kStart + row;
    std::string expected;
    for (vector_size_t item = 0; item < absolute % 4; ++item) {
      expected.push_back(static_cast<char>('a' + (absolute + item) % 26));
    }
    EXPECT_EQ(strings->valueAt(row).str(), expected);
  }
}

TEST_F(NativeLanceTest, decodesListOffsetsWithoutDivision) {
  constexpr uint64_t kAdjustment = 7;
  const std::array<uint64_t, 5> encoded{2, 9, 2, 5, 12};
  std::array<vector_size_t, encoded.size()> offsets;
  std::array<vector_size_t, encoded.size()> sizes;
  std::array<uint64_t, 1> nulls{bits::kNotNull64};

  const auto decoded = decodeLanceListOffsetsScalar(
      reinterpret_cast<const char*>(encoded.data()),
      encoded.size(),
      kAdjustment,
      0,
      offsets.data(),
      sizes.data(),
      nulls.data());

  EXPECT_EQ(offsets, (std::array<vector_size_t, 5>{0, 2, 2, 2, 5}));
  EXPECT_EQ(sizes, (std::array<vector_size_t, 5>{2, 0, 0, 3, 0}));
  EXPECT_EQ(decoded.lastItem, 5);
  EXPECT_TRUE(decoded.hasNulls);
  EXPECT_FALSE(bits::isBitNull(nulls.data(), 0));
  EXPECT_TRUE(bits::isBitNull(nulls.data(), 1));
  EXPECT_FALSE(bits::isBitNull(nulls.data(), 2));
  EXPECT_FALSE(bits::isBitNull(nulls.data(), 3));
  EXPECT_TRUE(bits::isBitNull(nulls.data(), 4));
}

TEST_F(NativeLanceTest, avx2ListOffsetsMatchScalar) {
  constexpr uint64_t kAdjustment = 101;
  constexpr uint64_t kRows = 37;
  std::array<uint64_t, kRows> encoded;
  uint64_t item = 7;
  for (uint64_t i = 0; i < kRows; ++i) {
    item += i % 5;
    encoded[i] = item + (i % 7 == 0 ? kAdjustment : 0);
  }
  std::array<vector_size_t, kRows> scalarOffsets;
  std::array<vector_size_t, kRows> scalarSizes;
  std::array<vector_size_t, kRows> avxOffsets;
  std::array<vector_size_t, kRows> avxSizes;
  std::array<uint64_t, 1> scalarNulls{bits::kNotNull64};
  std::array<uint64_t, 1> avxNulls{bits::kNotNull64};

  const auto scalar = decodeLanceListOffsetsScalar(
      reinterpret_cast<const char*>(encoded.data()),
      encoded.size(),
      kAdjustment,
      7,
      scalarOffsets.data(),
      scalarSizes.data(),
      scalarNulls.data());
  const auto avx2 = decodeLanceListOffsetsAvx2(
      reinterpret_cast<const char*>(encoded.data()),
      encoded.size(),
      kAdjustment,
      7,
      avxOffsets.data(),
      avxSizes.data(),
      avxNulls.data());

  EXPECT_EQ(avxOffsets, scalarOffsets);
  EXPECT_EQ(avxSizes, scalarSizes);
  EXPECT_EQ(avxNulls, scalarNulls);
  EXPECT_EQ(avx2.lastItem, scalar.lastItem);
  EXPECT_EQ(avx2.hasNulls, scalar.hasNulls);
}

TEST_F(NativeLanceTest, rejectsMultipleListNullAdjustments) {
  constexpr uint64_t kAdjustment = 7;
  const std::array<uint64_t, 1> encoded{15};
  std::array<vector_size_t, 1> offsets;
  std::array<vector_size_t, 1> sizes;
  EXPECT_THROW(
      decodeLanceListOffsetsScalar(
          reinterpret_cast<const char*>(encoded.data()),
          encoded.size(),
          kAdjustment,
          0,
          offsets.data(),
          sizes.data(),
          nullptr),
      BoltException);
}

TEST_F(NativeLanceTest, decodesPackedStructRange) {
  auto readFile = std::make_shared<InMemoryReadFile>(makePackedStructFile());
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(
      metadata.rowType()->toString(),
      "ROW<packed:ROW<x:BIGINT,y:INTEGER,z:SMALLINT>>");
  EXPECT_EQ(metadata.columns().size(), 1);
  EXPECT_EQ(metadata.physicalColumnSpan(0), 1);
  NativeLanceDecoder decoder(*input, metadata, *pool_);
  readFile->resetBytesRead();

  const auto values = decoder.decodeColumn(0, 1, 2);
  const auto* rows = values->as<RowVector>();
  ASSERT_NE(rows, nullptr);
  EXPECT_EQ(rows->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 101);
  EXPECT_EQ(rows->childAt(0)->asFlatVector<int64_t>()->valueAt(1), 102);
  EXPECT_EQ(rows->childAt(1)->asFlatVector<int32_t>()->valueAt(0), 201);
  EXPECT_EQ(rows->childAt(1)->asFlatVector<int32_t>()->valueAt(1), 202);
  EXPECT_EQ(rows->childAt(2)->asFlatVector<int16_t>()->valueAt(0), 11);
  EXPECT_EQ(rows->childAt(2)->asFlatVector<int16_t>()->valueAt(1), 12);
  EXPECT_EQ(readFile->bytesRead(), 2 * (sizeof(int64_t) + sizeof(int32_t) + 1));
}

TEST_F(NativeLanceTest, packedStructAvx2MatchesScalar) {
  constexpr uint64_t kRows = 1'027;
  constexpr uint32_t kColumns = 9;
  std::vector<uint64_t> packed(kRows * kColumns);
  for (uint64_t row = 0; row < kRows; ++row) {
    for (uint32_t column = 0; column < kColumns; ++column) {
      packed[row * kColumns + column] =
          (row + 1) * 0x9e3779b97f4a7c15ULL + column;
    }
  }

  std::array<std::array<uint64_t, kRows>, kColumns> scalar{};
  std::array<std::array<uint64_t, kRows>, kColumns> avx2{};
  std::array<NativeLancePackedStructColumn, kColumns> scalarColumns;
  std::array<NativeLancePackedStructColumn, kColumns> avx2Columns;
  for (uint32_t column = 0; column < kColumns; ++column) {
    scalarColumns[column] = {
        column * sizeof(uint64_t),
        sizeof(uint64_t),
        reinterpret_cast<uint8_t*>(scalar[column].data())};
    avx2Columns[column] = {
        column * sizeof(uint64_t),
        sizeof(uint64_t),
        reinterpret_cast<uint8_t*>(avx2[column].data())};
  }
  decodeLancePackedStructScalar(
      reinterpret_cast<const uint8_t*>(packed.data()),
      kRows,
      kColumns * sizeof(uint64_t),
      scalarColumns.data(),
      kColumns);
  decodeLancePackedStructAvx2(
      reinterpret_cast<const uint8_t*>(packed.data()),
      kRows,
      kColumns * sizeof(uint64_t),
      avx2Columns.data(),
      kColumns);
  EXPECT_EQ(avx2, scalar);
}

TEST_F(NativeLanceTest, decodesPackedNestedFixedSizeList) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makePackedNestedFixedSizeListFile()),
      *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto decoded = decoder.decodeColumn(0, 1, 1);
  const auto* row = decoded->as<RowVector>();
  ASSERT_NE(row, nullptr);
  const auto* outer = row->childAt(0)->as<ArrayVector>();
  ASSERT_NE(outer, nullptr);
  const auto* inner = outer->elements()->as<ArrayVector>();
  ASSERT_NE(inner, nullptr);
  const auto* values = inner->elements()->asFlatVector<int16_t>();
  ASSERT_NE(values, nullptr);
  ASSERT_EQ(values->size(), 4);
  EXPECT_EQ(values->valueAt(0), 5);
  EXPECT_EQ(values->valueAt(1), 6);
  EXPECT_EQ(values->valueAt(2), 7);
  EXPECT_EQ(values->valueAt(3), 8);
}

TEST_F(NativeLanceTest, decodesFsstSymbolsAndEscapes) {
  auto readFile = std::make_shared<InMemoryReadFile>(makeFsstFile());
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto values = decoder.decodeColumn(0, 1, 3);
  const auto* strings = values->asFlatVector<StringView>();
  EXPECT_EQ(strings->valueAt(0).str(), "world");
  EXPECT_EQ(strings->valueAt(1).str(), "!");
  EXPECT_EQ(strings->valueAt(2).str(), "hello");
}

TEST_F(NativeLanceTest, rowReaderPrefetchesWrappedEncodings) {
  for (const auto& contents :
       {makeBitpackedFile(),
        makeDictionaryFile(),
        makeFsstFile(),
        makeFixedSizeListFile(),
        makePackedStructFile()}) {
    auto input = std::make_unique<dwio::common::BufferedInput>(
        std::make_shared<InMemoryReadFile>(contents), *pool_);
    dwio::common::ReaderOptions readerOptions(pool_.get());
    NativeLanceReader reader(std::move(input), readerOptions);
    auto rowReader = reader.createRowReader({});
    VectorPtr result;
    EXPECT_GT(rowReader->next(2, result), 0);
    EXPECT_EQ(result->size(), 2);
  }
}

TEST_F(NativeLanceTest, decodesListAcrossPageBoundary) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeMultiPageListFile()), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto values = decoder.decodeColumn(0, 1, 3);
  const auto* arrays = values->as<ArrayVector>();
  ASSERT_NE(arrays, nullptr);
  EXPECT_EQ(arrays->sizeAt(0), 1);
  EXPECT_EQ(arrays->sizeAt(1), 3);
  EXPECT_EQ(arrays->sizeAt(2), 0);
  const auto* elements = arrays->elements()->asFlatVector<int32_t>();
  ASSERT_NE(elements, nullptr);
  EXPECT_EQ(elements->valueAt(0), 3);
  EXPECT_EQ(elements->valueAt(1), 4);
  EXPECT_EQ(elements->valueAt(2), 5);
  EXPECT_EQ(elements->valueAt(3), 6);

  const auto secondPage = decoder.decodeColumn(0, 3, 2);
  const auto* secondArrays = secondPage->as<ArrayVector>();
  ASSERT_NE(secondArrays, nullptr);
  EXPECT_EQ(secondArrays->sizeAt(0), 0);
  EXPECT_EQ(secondArrays->sizeAt(1), 2);
  EXPECT_EQ(secondArrays->elements()->asFlatVector<int32_t>()->valueAt(0), 7);
  EXPECT_EQ(secondArrays->elements()->asFlatVector<int32_t>()->valueAt(1), 8);
}

TEST_F(NativeLanceTest, decodesNestedLists) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeNestedListFile()), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(metadata.rowType()->toString(), "ROW<outer:ARRAY<ARRAY<INTEGER>>>");
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto decoded = decoder.decodeColumn(0, 1, 3);
  const auto* outer = decoded->as<ArrayVector>();
  ASSERT_NE(outer, nullptr);
  ASSERT_EQ(outer->size(), 3);
  EXPECT_EQ(outer->sizeAt(0), 0);
  EXPECT_EQ(outer->sizeAt(1), 2);
  EXPECT_EQ(outer->sizeAt(2), 1);
  const auto* inner = outer->elements()->as<ArrayVector>();
  ASSERT_NE(inner, nullptr);
  ASSERT_EQ(inner->size(), 3);
  EXPECT_EQ(inner->sizeAt(0), 2);
  EXPECT_EQ(inner->sizeAt(1), 1);
  EXPECT_EQ(inner->sizeAt(2), 1);
  const auto* values = inner->elements()->asFlatVector<int32_t>();
  ASSERT_NE(values, nullptr);
  ASSERT_EQ(values->size(), 4);
  EXPECT_EQ(values->valueAt(0), 3);
  EXPECT_EQ(values->valueAt(1), 4);
  EXPECT_EQ(values->valueAt(2), 5);
  EXPECT_EQ(values->valueAt(3), 6);
}

TEST_F(NativeLanceTest, decodesMapRangeWithNullAndEmptyRows) {
  auto readFile = std::make_shared<InMemoryReadFile>(makeMapFile());
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_EQ(
      metadata.rowType()->toString(), "ROW<attributes:MAP<INTEGER,BIGINT>>");
  EXPECT_EQ(metadata.columns().size(), 4);
  EXPECT_EQ(metadata.physicalColumnSpan(0), 4);
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto values = decoder.decodeColumn(0, 1, 3);
  const auto* maps = values->as<MapVector>();
  ASSERT_NE(maps, nullptr) << values->toString();
  ASSERT_EQ(maps->size(), 3);
  EXPECT_TRUE(maps->isNullAt(0));
  EXPECT_FALSE(maps->isNullAt(1));
  EXPECT_EQ(maps->sizeAt(1), 0);
  EXPECT_FALSE(maps->isNullAt(2));
  EXPECT_EQ(maps->sizeAt(2), 1);
  ASSERT_EQ(maps->mapKeys()->size(), 1);
  ASSERT_EQ(maps->mapValues()->size(), 1);
  EXPECT_EQ(maps->mapKeys()->asFlatVector<int32_t>()->valueAt(0), 3);
  EXPECT_EQ(maps->mapValues()->asFlatVector<int64_t>()->valueAt(0), 30);
}

TEST_F(NativeLanceTest, rowReaderDecodesMap) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeMapFile()), *pool_);
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(std::move(input), readerOptions);
  auto rowReader = reader.createRowReader({});

  VectorPtr result;
  EXPECT_EQ(rowReader->next(5, result), 5);
  const auto* maps = result->as<RowVector>()->childAt(0)->as<MapVector>();
  ASSERT_NE(maps, nullptr);
  EXPECT_EQ(maps->sizeAt(0), 2);
  EXPECT_TRUE(maps->isNullAt(1));
  EXPECT_EQ(maps->sizeAt(2), 0);
  EXPECT_EQ(maps->sizeAt(3), 1);
  EXPECT_EQ(maps->sizeAt(4), 2);
  const auto* keys = maps->mapKeys()->asFlatVector<int32_t>();
  const auto* values = maps->mapValues()->asFlatVector<int64_t>();
  for (vector_size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(keys->valueAt(i), i + 1);
    EXPECT_EQ(values->valueAt(i), (i + 1) * 10);
  }
}

TEST_F(NativeLanceTest, decodesBlobColumnAcrossPages) {
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(makeBlobFile()), *pool_);
  NativeLanceMetadata metadata(*input, *pool_);
  EXPECT_TRUE(metadata.isBlobColumn(0));
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const auto decoded = decoder.decodeColumn(0, 1, 3);
  const auto* values = decoded->asFlatVector<StringView>();
  ASSERT_NE(values, nullptr);
  ASSERT_EQ(values->size(), 3);
  EXPECT_TRUE(values->isNullAt(0));
  EXPECT_FALSE(values->isNullAt(1));
  EXPECT_EQ(values->valueAt(1).size(), 0);
  EXPECT_FALSE(values->isNullAt(2));
  EXPECT_EQ(values->valueAt(2).str(), "xyz");
}

TEST_F(NativeLanceTest, prefetchConsumesScheduledStreamsWithoutRereading) {
  auto readFile =
      std::make_shared<CountingReadFile>(makeBooleanTimestampFile());
  auto input = std::make_unique<dwio::common::BufferedInput>(
      readFile, *pool_, dwio::common::MetricsLog::voidLog(), nullptr, 0);
  NativeLanceMetadata metadata(*input, *pool_);
  const auto metadataReads = readFile->readCalls();
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  decoder.prefetchColumns({0, 1}, 0, 5);
  const auto afterPrefetch = readFile->readCalls();
  EXPECT_EQ(afterPrefetch, metadataReads + 2);
  EXPECT_EQ(decoder.decodeColumn(0, 0, 5)->size(), 5);
  EXPECT_EQ(decoder.decodeColumn(1, 0, 5)->size(), 5);
  EXPECT_EQ(readFile->readCalls(), afterPrefetch);
}

TEST_F(NativeLanceTest, readPlanDeduplicatesAndServesSubmittedSubranges) {
  auto readFile =
      std::make_shared<CountingReadFile>("abcdefghijklmnopqrstuvwxyz");
  auto input = std::make_unique<dwio::common::BufferedInput>(
      readFile, *pool_, dwio::common::MetricsLog::voidLog(), nullptr, 0);
  NativeLanceReadPlan plan(*pool_);

  plan.schedule(*input, 2, 10);
  plan.schedule(*input, 2, 10);
  plan.submit(*input);
  EXPECT_EQ(readFile->readCalls(), 1);

  const auto subrange = plan.take(4, 3);
  ASSERT_NE(subrange, nullptr);
  EXPECT_EQ(std::string(subrange->as<char>(), subrange->size()), "efg");
  EXPECT_EQ(readFile->readCalls(), 1);

  const auto exact = plan.take(2, 10);
  ASSERT_NE(exact, nullptr);
  EXPECT_EQ(std::string(exact->as<char>(), exact->size()), "cdefghijkl");
  EXPECT_EQ(readFile->readCalls(), 1);
}

TEST_F(NativeLanceTest, readPlanDoesNotMatchRangesPastPrefetchedEnd) {
  auto readFile =
      std::make_shared<CountingReadFile>("abcdefghijklmnopqrstuvwxyz");
  auto input = std::make_unique<dwio::common::BufferedInput>(
      readFile, *pool_, dwio::common::MetricsLog::voidLog(), nullptr, 0);
  NativeLanceReadPlan plan(*pool_);

  plan.schedule(*input, 2, 3);
  plan.load(*input);
  EXPECT_EQ(readFile->readCalls(), 1);
  EXPECT_EQ(plan.take(5, 1), nullptr);
}

TEST_F(NativeLanceTest, readPlanChunksAndBoundsInFlightBytes) {
  auto readFile = std::make_shared<CountingReadFile>(
      "abcdefghijklmnopqrstuvwxyz0123456789");
  auto input = std::make_unique<TrackingBufferedInput>(readFile, *pool_);
  NativeLanceReadPlan plan(*pool_, {.maxReadBytes = 4, .maxInFlightBytes = 8});

  plan.schedule(*input, 3, 17);
  plan.submit(*input);
  EXPECT_LE(plan.peakInFlightBytes(), 8);
  EXPECT_EQ(input->loadCalls, 3);
  ASSERT_EQ(input->enqueuedRegions.size(), 5);
  EXPECT_EQ(
      (std::vector<uint64_t>{4, 4, 4, 4, 1}),
      (std::vector<uint64_t>{
          input->enqueuedRegions[0].length,
          input->enqueuedRegions[1].length,
          input->enqueuedRegions[2].length,
          input->enqueuedRegions[3].length,
          input->enqueuedRegions[4].length}));

  const auto result = plan.take(3, 17);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(
      std::string(result->as<char>(), result->size()), "defghijklmnopqrst");
  EXPECT_EQ(readFile->bytesRead(), 17);
}

TEST_F(NativeLanceTest, readPlanCancellationIsIdempotent) {
  auto readFile =
      std::make_shared<CountingReadFile>("abcdefghijklmnopqrstuvwxyz");
  auto input = std::make_unique<TrackingBufferedInput>(readFile, *pool_);
  NativeLanceReadPlan plan(*pool_, {.maxReadBytes = 4, .maxInFlightBytes = 8});

  plan.schedule(*input, 2, 12);
  plan.cancel(input.get());
  plan.cancel(input.get());
  EXPECT_EQ(input->cancelCalls, 1);
  EXPECT_TRUE(plan.cancelled());
  EXPECT_EQ(plan.inFlightBytes(), 0);
  EXPECT_EQ(plan.take(2, 12), nullptr);
  EXPECT_EQ(readFile->readCalls(), 0);
  EXPECT_THROW(plan.schedule(*input, 0, 1), BoltException);
  EXPECT_THROW(plan.submit(*input), BoltException);
}

TEST_F(NativeLanceTest, metadataCoalescesColumnDescriptors) {
  auto readFile =
      std::make_shared<CountingReadFile>(makeBooleanTimestampFile());
  auto input = std::make_unique<dwio::common::BufferedInput>(
      readFile, *pool_, dwio::common::MetricsLog::voidLog(), nullptr, 0);
  NativeLanceMetadata metadata(*input, *pool_);

  EXPECT_EQ(metadata.columns().size(), 3);
  // Footer, global table, schema, column table, and one coalesced read for all
  // adjacent column descriptors.
  EXPECT_EQ(readFile->readCalls(), 5);
}

TEST_F(NativeLanceTest, directBufferedInputConsumesCoalescedPlanOnce) {
  auto readFile =
      std::make_shared<CountingReadFile>(makeBooleanTimestampFile());
  auto ioStats = std::make_shared<io::IoStatistics>();
  dwio::common::ReaderOptions readerOptions(pool_.get());
  readerOptions.setLoadQuantum(64 * 1024);
  auto input = std::make_unique<dwio::common::DirectBufferedInput>(
      readFile,
      dwio::common::MetricsLog::voidLog(),
      1,
      nullptr,
      1,
      ioStats,
      nullptr,
      readerOptions,
      nullptr);
  NativeLanceReader reader(std::move(input), readerOptions);
  const auto readsAfterMetadata = readFile->readCalls();

  auto rowReader = reader.createRowReader({});
  VectorPtr result;
  EXPECT_EQ(rowReader->next(5, result), 5);
  // The three requested columns are planned together. DirectBufferedInput
  // coalesces their nearby data buffers into one physical read.
  EXPECT_EQ(readFile->readCalls(), readsAfterMetadata + 1);
}

TEST_F(NativeLanceTest, selectedRangesShareOneDirectBufferedInputLoad) {
  auto readFile =
      std::make_shared<CountingReadFile>(makeBooleanTimestampFile());
  auto ioStats = std::make_shared<io::IoStatistics>();
  dwio::common::ReaderOptions readerOptions(pool_.get());
  readerOptions.setLoadQuantum(64 * 1024);
  auto input = std::make_unique<dwio::common::DirectBufferedInput>(
      readFile,
      dwio::common::MetricsLog::voidLog(),
      1,
      nullptr,
      1,
      ioStats,
      nullptr,
      readerOptions,
      nullptr);
  NativeLanceMetadata metadata(*input, *pool_);
  const auto readsAfterMetadata = readFile->readCalls();
  NativeLanceDecoder decoder(*input, metadata, *pool_);

  const std::array<vector_size_t, 3> rows{0, 2, 4};
  const auto result = decoder.decodeSelectedRows(1, 0, rows);
  ASSERT_EQ(result->size(), rows.size());
  const auto* timestamps = result->asFlatVector<Timestamp>();
  EXPECT_EQ(timestamps->valueAt(0), Timestamp::fromMicros(-1));
  EXPECT_EQ(timestamps->valueAt(1), Timestamp::fromMicros(1));
  EXPECT_EQ(timestamps->valueAt(2), Timestamp::fromMicros(2'000'002));
  EXPECT_EQ(readFile->readCalls(), readsAfterMetadata + 1);
}

TEST_F(NativeLanceTest, rowReaderPrefetchUnitsFollowBatchCap) {
  auto readFile =
      std::make_shared<CountingReadFile>(makeBooleanTimestampFile());
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(std::move(input), readerOptions);

  dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.select(std::make_shared<dwio::common::ColumnSelector>(
      reader.rowType(), std::vector<std::string>{"flag"}));
  rowReaderOptions.setMaxBatchBytes(4);
  auto rowReader = reader.createRowReader(rowReaderOptions);

  auto prefetchUnits = rowReader->prefetchUnits();
  ASSERT_TRUE(prefetchUnits.has_value());
  ASSERT_EQ(prefetchUnits->size(), 3);
  EXPECT_EQ((*prefetchUnits)[0].rowCount, 2);
  EXPECT_EQ((*prefetchUnits)[1].rowCount, 2);
  EXPECT_EQ((*prefetchUnits)[2].rowCount, 1);
  EXPECT_TRUE(rowReader->allPrefetchIssued());

  const auto readsAfterMetadata = readFile->readCalls();
  EXPECT_EQ(
      (*prefetchUnits)[0].prefetch(),
      dwio::common::RowReader::FetchResult::kFetched);
  const auto readsAfterPrefetch = readFile->readCalls();
  EXPECT_GT(readsAfterPrefetch, readsAfterMetadata);
  EXPECT_EQ(
      (*prefetchUnits)[0].prefetch(),
      dwio::common::RowReader::FetchResult::kAlreadyFetched);
  EXPECT_EQ(readFile->readCalls(), readsAfterPrefetch);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(20, result), 2);
  EXPECT_EQ(readFile->readCalls(), readsAfterPrefetch);
  ASSERT_EQ(result->size(), 2);
  const auto* rows = result->as<RowVector>();
  ASSERT_NE(rows, nullptr);
  const auto* values = rows->childAt(0)->asFlatVector<bool>();
  ASSERT_NE(values, nullptr);
  EXPECT_TRUE(values->valueAt(0));
  EXPECT_TRUE(values->isNullAt(1));

  for (size_t i = 1; i < prefetchUnits->size(); ++i) {
    EXPECT_EQ(
        (*prefetchUnits)[i].prefetch(),
        dwio::common::RowReader::FetchResult::kFetched);
  }
  EXPECT_TRUE(rowReader->allPrefetchIssued());
}

TEST_F(NativeLanceTest, rowReaderPrefetchSubmitsDirectInputPlan) {
  auto readFile =
      std::make_shared<CountingReadFile>(makeBooleanTimestampFile());
  auto ioStats = std::make_shared<io::IoStatistics>();
  dwio::common::ReaderOptions readerOptions(pool_.get());
  readerOptions.setLoadQuantum(64 * 1024);
  auto input = std::make_unique<dwio::common::DirectBufferedInput>(
      readFile,
      dwio::common::MetricsLog::voidLog(),
      1,
      nullptr,
      1,
      ioStats,
      nullptr,
      readerOptions,
      nullptr);
  NativeLanceReader reader(std::move(input), readerOptions);

  dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.select(std::make_shared<dwio::common::ColumnSelector>(
      reader.rowType(), std::vector<std::string>{"ts"}));
  rowReaderOptions.setMaxBatchBytes(40);
  auto rowReader = reader.createRowReader(rowReaderOptions);

  auto prefetchUnits = rowReader->prefetchUnits();
  ASSERT_TRUE(prefetchUnits.has_value());
  ASSERT_EQ(prefetchUnits->size(), 3);
  EXPECT_EQ((*prefetchUnits)[0].rowCount, 2);

  const auto readsAfterMetadata = readFile->readCalls();
  EXPECT_EQ(
      (*prefetchUnits)[0].prefetch(),
      dwio::common::RowReader::FetchResult::kFetched);
  EXPECT_EQ(readFile->readCalls(), readsAfterMetadata);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(20, result), 2);
  EXPECT_EQ(readFile->readCalls(), readsAfterMetadata + 1);
  const auto* timestamps =
      result->as<RowVector>()->childAt(0)->asFlatVector<Timestamp>();
  ASSERT_NE(timestamps, nullptr);
  EXPECT_EQ(timestamps->valueAt(0), Timestamp::fromMicros(-1));
  EXPECT_EQ(timestamps->valueAt(1), Timestamp::fromMicros(0));
}

TEST_F(NativeLanceTest, rowReaderPipelinesNextDirectInputBatch) {
  auto readFile =
      std::make_shared<CountingReadFile>(makeBooleanTimestampFile());
  auto ioStats = std::make_shared<io::IoStatistics>();
  dwio::common::ReaderOptions readerOptions(pool_.get());
  readerOptions.setLoadQuantum(64 * 1024);
  auto input = std::make_unique<dwio::common::DirectBufferedInput>(
      readFile,
      dwio::common::MetricsLog::voidLog(),
      1,
      nullptr,
      1,
      ioStats,
      nullptr,
      readerOptions,
      nullptr);
  NativeLanceReader reader(std::move(input), readerOptions);

  dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.select(std::make_shared<dwio::common::ColumnSelector>(
      reader.rowType(), std::vector<std::string>{"ts"}));
  rowReaderOptions.setMaxBatchBytes(40);
  auto rowReader = reader.createRowReader(rowReaderOptions);
  auto units = rowReader->prefetchUnits();
  ASSERT_TRUE(units.has_value());
  ASSERT_EQ(units->size(), 3);

  const auto readsAfterMetadata = readFile->readCalls();
  VectorPtr result;
  EXPECT_EQ(rowReader->next(20, result), 2);
  EXPECT_EQ(readFile->readCalls(), readsAfterMetadata + 1);
  EXPECT_EQ(
      (*units)[1].prefetch(),
      dwio::common::RowReader::FetchResult::kAlreadyFetched);

  EXPECT_EQ(rowReader->next(20, result), 2);
  EXPECT_EQ(readFile->readCalls(), readsAfterMetadata + 2);
  EXPECT_EQ(
      (*units)[2].prefetch(),
      dwio::common::RowReader::FetchResult::kAlreadyFetched);

  EXPECT_EQ(rowReader->next(20, result), 1);
  EXPECT_EQ(readFile->readCalls(), readsAfterMetadata + 3);
}

TEST_F(NativeLanceTest, rowReaderOnlyReadsProjectedColumn) {
  auto readFile = std::make_shared<LocalReadFile>("examples/sample.lance");
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool_);
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(std::move(input), readerOptions);
  readFile->resetBytesRead();

  dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.select(std::make_shared<dwio::common::ColumnSelector>(
      reader.rowType(), std::vector<std::string>{"b"}));
  auto rowReader = reader.createRowReader(rowReaderOptions);
  VectorPtr result;
  EXPECT_EQ(rowReader->next(4, result), 4);
  EXPECT_EQ(readFile->bytesRead(), 4 * sizeof(double));
}

TEST_F(NativeLanceTest, rowReaderDecodesColumnsInParallel) {
  constexpr uint32_t kColumns = 16;
  auto input = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<InMemoryReadFile>(
          makeManyCompressedColumnsFile(kColumns)),
      *pool_);
  NativeLanceReader reader(
      std::move(input), dwio::common::ReaderOptions(pool_.get()));
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(2);
  dwio::common::RowReaderOptions options;
  options.setDecodingExecutor(executor);
  options.setDecodingParallelismFactor(3);
  auto rowReader = reader.createRowReader(options);

  VectorPtr result;
  EXPECT_EQ(rowReader->next(8, result), 8);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->size(), 8);
  EXPECT_TRUE(result->type()->equivalent(*reader.rowType()));
  const auto* row = result->as<RowVector>();
  ASSERT_NE(row, nullptr);
  ASSERT_EQ(row->childrenSize(), kColumns);
  for (uint32_t column = 0; column < kColumns; ++column) {
    const auto* values = row->childAt(column)->asFlatVector<int32_t>();
    for (uint64_t i = 0; i < 8; ++i) {
      EXPECT_EQ(values->valueAt(i), column * 100 + i);
    }
  }
}

TEST_F(NativeLanceTest, rowReaderHonorsBatchMemoryBudget) {
  dwio::common::ReaderOptions readerOptions(pool_.get());
  NativeLanceReader reader(openFile("sample.lance", *pool_), readerOptions);
  dwio::common::RowReaderOptions rowReaderOptions;
  rowReaderOptions.select(std::make_shared<dwio::common::ColumnSelector>(
      reader.rowType(), std::vector<std::string>{"a"}));
  rowReaderOptions.setMaxBatchBytes(18);
  auto rowReader = reader.createRowReader(rowReaderOptions);

  ASSERT_TRUE(rowReader->estimatedRowSize().has_value());
  EXPECT_GE(*rowReader->estimatedRowSize(), sizeof(int64_t));
  EXPECT_EQ(rowReader->nextReadSize(20), 2);
  VectorPtr result;
  EXPECT_EQ(rowReader->next(20, result), 2);
  ASSERT_EQ(result->size(), 2);
  EXPECT_EQ(
      result->as<RowVector>()->childAt(0)->asFlatVector<int64_t>()->valueAt(0),
      1);
  EXPECT_EQ(
      result->as<RowVector>()->childAt(0)->asFlatVector<int64_t>()->valueAt(1),
      2);

  dwio::common::RuntimeStatistics stats;
  rowReader->updateRuntimeStats(stats);
  EXPECT_EQ(stats.processedStrides, 1);
  EXPECT_GT(stats.decodeTimeNs, 0);
}

} // namespace
} // namespace bytedance::bolt::lance::reader::test
