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

#include <cstdint>
#include <memory>
#include <string>

#include "bolt/dwio/common/BufferedInput.h"

namespace bytedance::bolt::lance::reader {

/// Resolves the object referenced by a non-inline Lance Blob v2 descriptor.
///
/// The reader deliberately does not embed dataset-manifest or filesystem
/// policy. A dataset integration supplies this interface and returns an input
/// configured with its cache, executor, accounting, and coalescing policy.
class NativeLanceBlobResolver {
 public:
  enum class Kind : uint8_t { kPacked = 1, kDedicated = 2, kExternal = 3 };

  struct Request {
    Kind kind;
    std::string sourceDataFile;
    uint32_t blobId;
    std::string uri;
    uint64_t position;
    uint64_t size;
  };

  virtual ~NativeLanceBlobResolver() = default;

  /// This method must be thread-safe because row prefetch can call it from I/O
  /// executor threads. For Packed and Dedicated descriptors, implementations
  /// normally resolve blobId relative to sourceDataFile. For External
  /// descriptors, they resolve uri directly or against the dataset base
  /// identified by blobId.
  virtual std::unique_ptr<dwio::common::BufferedInput> resolve(
      const Request& request,
      memory::MemoryPool& pool) const = 0;
};

} // namespace bytedance::bolt::lance::reader
