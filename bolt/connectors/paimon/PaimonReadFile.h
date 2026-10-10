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

#include <folly/Range.h>
#include <folly/io/IOBuf.h>
#include <paimon/fs/file_system.h>
#include <limits>
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
    readFully(static_cast<char*>(buf), offset, length);
    bytesRead_ += length;
    return std::string_view(static_cast<const char*>(buf), length);
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    uint64_t total = 0;
    uint64_t off = offset;
    for (const auto& range : buffers) {
      validateReadRange(off, range.size());
      if (range.data()) {
        readFully(range.data(), off, range.size());
        total += range.size();
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
      validateReadRange(r.offset, r.length);
      auto& buf = iobufs[i];
      buf = folly::IOBuf(folly::IOBuf::CREATE, r.length);
      auto* data = reinterpret_cast<char*>(buf.writableData());
      readFully(data, r.offset, r.length);
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
    if (res.value() < 0) {
      throw std::runtime_error("Negative length from Paimon InputStream");
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
  static void validateReadRange(uint64_t offset, uint64_t length) {
    constexpr auto kMax = std::numeric_limits<int64_t>::max();
    if (offset > kMax || length > kMax - offset) {
      throw std::runtime_error("Paimon read range exceeds INT64_MAX");
    }
  }

  void readFully(char* buffer, uint64_t offset, uint64_t length) const {
    validateReadRange(offset, length);
    auto res = input_->Read(
        buffer, static_cast<int64_t>(length), static_cast<int64_t>(offset));
    if (!res.ok()) {
      throw std::runtime_error(res.status().ToString());
    }
    if (res.value() != static_cast<int64_t>(length)) {
      throw std::runtime_error("Short read from Paimon InputStream");
    }
  }

  std::shared_ptr<::paimon::InputStream> input_;
  PaimonIoOptions ioOptions_;
};

} // namespace bytedance::bolt::connector::paimon
