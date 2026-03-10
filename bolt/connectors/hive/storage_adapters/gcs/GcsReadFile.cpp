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

#include "bolt/connectors/hive/storage_adapters/gcs/GcsReadFile.h"
#include "bolt/connectors/hive/storage_adapters/gcs/GcsUtil.h"

namespace bytedance::bolt::filesystems {

namespace gcs = ::google::cloud::storage;

class GcsReadFile::Impl {
 public:
  Impl(const std::string& path, std::shared_ptr<gcs::Client> client)
      : client_(client) {
    setBucketAndKeyFromGcsPath(path, bucket_, key_);
  }

  // Gets the length of the file.
  // Checks if there are any issues reading the file.
  void initialize() {
    // get metadata and initialize length
    auto metadata = client_->GetObjectMetadata(bucket_, key_);
    if (!metadata.ok()) {
      checkGcsStatus(
          metadata.status(),
          "Failed to get metadata for GCS object",
          bucket_,
          key_);
    }
    length_ = (*metadata).size();
    BOLT_CHECK_GE(length_, 0);
  }

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buffer,
      std::atomic<uint64_t>& bytesRead) const {
    preadInternal(offset, length, static_cast<char*>(buffer), bytesRead);
    return {static_cast<char*>(buffer), length};
  }

  std::string pread(
      uint64_t offset,
      uint64_t length,
      std::atomic<uint64_t>& bytesRead) const {
    std::string result(length, 0);
    char* position = result.data();
    preadInternal(offset, length, position, bytesRead);
    return result;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      std::atomic<uint64_t>& bytesRead) const {
    // 'buffers' contains Ranges(data, size)  with some gaps (data = nullptr) in
    // between. This call must populate the ranges (except gap ranges)
    // sequentially starting from 'offset'. If a range pointer is nullptr, the
    // data from stream of size range.size() will be skipped.
    size_t length = 0;
    for (const auto range : buffers) {
      length += range.size();
    }
    std::string result(length, 0);
    preadInternal(offset, length, static_cast<char*>(result.data()), bytesRead);
    size_t resultOffset = 0;
    for (auto range : buffers) {
      if (range.data()) {
        memcpy(range.data(), &(result.data()[resultOffset]), range.size());
      }
      resultOffset += range.size();
    }
    return length;
  }

  uint64_t size() const {
    return length_;
  }

  uint64_t memoryUsage() const {
    return sizeof(GcsReadFile) // this class
        + sizeof(gcs::Client) // pointee
        + kUploadBufferSize; // buffer size
  }

  std::string getName() const {
    return key_;
  }

 private:
  // The assumption here is that "position" has space for at least "length"
  // bytes.
  void preadInternal(
      uint64_t offset,
      uint64_t length,
      char* position,
      std::atomic<uint64_t>& bytesRead_) const {
    gcs::ObjectReadStream stream = client_->ReadObject(
        bucket_, key_, gcs::ReadRange(offset, offset + length));
    if (!stream) {
      checkGcsStatus(
          stream.status(), "Failed to get GCS object", bucket_, key_);
    }

    stream.read(position, length);
    if (!stream) {
      checkGcsStatus(
          stream.status(), "Failed to get read object", bucket_, key_);
    }
    bytesRead_ += length;
  }

  std::shared_ptr<gcs::Client> client_;
  std::string bucket_;
  std::string key_;
  std::atomic<int64_t> length_ = -1;
};

GcsReadFile::GcsReadFile(
    const std::string& path,
    std::shared_ptr<gcs::Client> client)
    : impl_(std::make_unique<Impl>(path, client)) {}

GcsReadFile::~GcsReadFile() = default;

void GcsReadFile::initialize() {
  impl_->initialize();
}

std::string_view
GcsReadFile::pread(uint64_t offset, uint64_t length, void* buffer) const {
  return impl_->pread(offset, length, buffer, bytesRead_);
}

std::string GcsReadFile::pread(uint64_t offset, uint64_t length) const {
  return impl_->pread(offset, length, bytesRead_);
}
uint64_t GcsReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) const {
  return impl_->preadv(offset, buffers, bytesRead_);
}

uint64_t GcsReadFile::size() const {
  return impl_->size();
}

uint64_t GcsReadFile::memoryUsage() const {
  return impl_->memoryUsage();
}

std::string GcsReadFile::getName() const {
  return impl_->getName();
}

uint64_t GcsReadFile::getNaturalReadSize() const {
  return kUploadBufferSize;
}

} // namespace bytedance::bolt::filesystems
