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

#pragma once

#include <memory>
#include <string>

#include <folly/Range.h>
#include <folly/io/IOBuf.h>
#include <paimon/format/file_format.h>
#include <paimon/format/file_format_factory.h>
#include <paimon/fs/file_system.h>
#include "bolt/common/file/File.h"
#include "bolt/common/file/Region.h"
#include "bolt/connectors/paimon/PaimonConfig.h"

namespace bytedance::bolt::connector::paimon {

// Adapter to wrap a paimon::InputStream in Bolt's ReadFile interface
class PaimonReadFile : public bolt::ReadFile {
 public:
  explicit PaimonReadFile(
      std::shared_ptr<::paimon::InputStream> is,
      const PaimonIoOptions& ioOptions)
      : input_(std::move(is)), ioOptions_(ioOptions) {}

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const override {
    auto res = input_->Read(
        static_cast<char*>(buf), static_cast<uint32_t>(length), offset);
    if (!res.ok()) {
      throw std::runtime_error(res.status().ToString());
    }
    const auto readBytes = static_cast<uint64_t>(res.value());
    if (readBytes != length) {
      throw std::runtime_error("Short read from Paimon InputStream");
    }
    bytesRead_ += length;
    return std::string_view(static_cast<const char*>(buf), length);
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    uint64_t total = 0;
    uint64_t off = offset;
    for (const auto& range : buffers) {
      if (range.data()) {
        auto res = input_->Read(
            range.data(), static_cast<uint32_t>(range.size()), off);
        if (!res.ok()) {
          throw std::runtime_error(res.status().ToString());
        }
        const auto readBytes = static_cast<uint64_t>(res.value());
        if (readBytes != range.size()) {
          throw std::runtime_error("Short read from Paimon InputStream");
        }
        total += readBytes;
      }
      off += range.size();
    }
    bytesRead_ += total;
    return total;
  }

  void preadv(
      folly::Range<const bolt::common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs) const override {
    if (regions.size() != iobufs.size()) {
      throw std::runtime_error("regions and iobufs size mismatch");
    }
    for (size_t i = 0; i < regions.size(); ++i) {
      const auto& r = regions[i];
      auto& buf = iobufs[i];
      buf = folly::IOBuf(folly::IOBuf::CREATE, r.length);
      auto* data = reinterpret_cast<char*>(buf.writableData());
      auto res = input_->Read(data, static_cast<uint32_t>(r.length), r.offset);
      if (!res.ok()) {
        throw std::runtime_error(res.status().ToString());
      }
      const auto readBytes = static_cast<uint64_t>(res.value());
      if (readBytes != r.length) {
        throw std::runtime_error("Short read from Paimon InputStream");
      }
      buf.append(r.length);
      bytesRead_ += r.length;
    }
  }

  bool hasPreadvAsync() const override {
    return false;
  }

  bool shouldCoalesce() const override {
    return ioOptions_.coalesceReads;
  }

  uint64_t size() const override {
    auto res = input_->Length();
    if (!res.ok()) {
      throw std::runtime_error(res.status().ToString());
    }
    return res.value();
  }

  uint64_t memoryUsage() const override {
    return 0;
  }

  std::string getName() const override {
    auto res = input_->GetUri();
    if (res.ok()) {
      return res.value();
    }
    return std::string("<PaimonInputStream>");
  }

  uint64_t getNaturalReadSize() const override {
    return ioOptions_.naturalReadSize;
  }

 private:
  std::shared_ptr<::paimon::InputStream> input_;
  PaimonIoOptions ioOptions_;
};

class PaimonParquetReader : public ::paimon::FileFormat {
 public:
  explicit PaimonParquetReader(
      const std::map<std::string, std::string>& options);

  const std::string& Identifier() const override;

  ::paimon::Result<std::unique_ptr<::paimon::ReaderBuilder>>
  CreateReaderBuilder(int32_t batch_size) const override;

  ::paimon::Result<std::unique_ptr<::paimon::WriterBuilder>>
  CreateWriterBuilder(::ArrowSchema* schema, int32_t batch_size) const override;

  ::paimon::Result<std::unique_ptr<::paimon::FormatStatsExtractor>>
  CreateStatsExtractor(::ArrowSchema* schema) const override;

 private:
  PaimonIoOptions ioOptions_;
  uint8_t timestampPrecision_ = 3; // default: milliseconds (matches hive)
};

// Ensures that paimon's FileFormatFactory for "parquet" (backed by
// bolt's native parquet reader) is registered
void EnsurePaimonParquetFormatRegistered();

} // namespace bytedance::bolt::connector::paimon

namespace paimon {

class ParquetFileFormatFactory : public ::paimon::FileFormatFactory {
 public:
  static constexpr char kIDENTIFIER[] = "parquet";

  const char* Identifier() const override {
    return kIDENTIFIER;
  }

  ::paimon::Result<std::unique_ptr<::paimon::FileFormat>> Create(
      const std::map<std::string, std::string>& options) const override;
};

void ensureParquetFormatFactoryRegistered();

} // namespace paimon
