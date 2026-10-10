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

#include "bolt/common/memory/bm/BlockDescriptor.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace bytedance::bolt::memory::bm::compress {

struct OpenZlCompressor {
  OpenZlCompressor(void* native, std::string fingerprint);
  ~OpenZlCompressor();

  OpenZlCompressor(const OpenZlCompressor&) = delete;
  OpenZlCompressor& operator=(const OpenZlCompressor&) = delete;

  void* native{nullptr};
  std::string fingerprint;
};

struct OpenZlCompressionContext {
  OpenZlCompressionContext();
  ~OpenZlCompressionContext();

  OpenZlCompressionContext(const OpenZlCompressionContext&) = delete;
  OpenZlCompressionContext& operator=(const OpenZlCompressionContext&) = delete;

  void* native{nullptr};
};

struct OpenZlDecompressionContext {
  OpenZlDecompressionContext();
  ~OpenZlDecompressionContext();

  OpenZlDecompressionContext(const OpenZlDecompressionContext&) = delete;
  OpenZlDecompressionContext& operator=(const OpenZlDecompressionContext&) =
      delete;

  void* native{nullptr};
};

struct OpenZlCompressionResult {
  size_t size{0};
  bool capacityTooSmall{false};
};

bool OpenZlSupportsDescriptor(
    const BlockDescriptor& descriptor,
    size_t blockSize);

std::string OpenZlDescriptorFingerprint(
    const BlockDescriptor& descriptor,
    size_t blockSize);

std::shared_ptr<const OpenZlCompressor> BuildOpenZlCompressor(
    const BlockDescriptor& descriptor,
    size_t blockSize,
    std::string fingerprint);

size_t OpenZlInitialCompressedCapacity(size_t rawSize);

OpenZlCompressionResult OpenZlCompress(
    OpenZlCompressionContext& context,
    const OpenZlCompressor& compressor,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity);

void OpenZlDecompress(
    OpenZlDecompressionContext& context,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize,
    uint64_t blockId);

} // namespace bytedance::bolt::memory::bm::compress
