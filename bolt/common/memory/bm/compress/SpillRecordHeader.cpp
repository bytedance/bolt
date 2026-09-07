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

#include "bolt/common/memory/bm/compress/SpillRecordHeader.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/compress/CompressionAlgorithm.h"

#include <cstring>

namespace bytedance::bolt::memory::bm::compress {

namespace {

bool isSupportedKind(uint32_t kind) {
  return SupportedCompressionKind(static_cast<CompressionKind>(kind));
}

} // namespace

std::array<char, sizeof(SpillRecordHeader)> EncodeSpillRecordHeader(
    const SpillRecordHeader& input) {
  auto header = input;
  header.magic = kSpillRecordMagic;
  header.version = kSpillRecordVersion;
  header.headerSize = sizeof(SpillRecordHeader);
  std::array<char, sizeof(SpillRecordHeader)> encoded{};
  std::memcpy(encoded.data(), &header, sizeof(header));
  return encoded;
}

SpillRecordHeader DecodeSpillRecordHeader(
    const char* data,
    size_t size,
    uint64_t expectedRawSize) {
  BOLT_CHECK_NOT_NULL(data);
  if (size < sizeof(SpillRecordHeader)) {
    BOLT_FAIL(
        "BM spill record header too small, size={}, expected_at_least={}",
        size,
        sizeof(SpillRecordHeader));
  }

  SpillRecordHeader header;
  std::memcpy(&header, data, sizeof(header));
  if (header.magic != kSpillRecordMagic) {
    BOLT_FAIL("BM spill record header has invalid magic={}", header.magic);
  }
  if (header.version != kSpillRecordVersion) {
    BOLT_FAIL(
        "BM spill record header has unsupported version={}", header.version);
  }
  if (header.headerSize < sizeof(SpillRecordHeader)) {
    BOLT_FAIL(
        "BM spill record header size too small, header_size={}, expected={}",
        header.headerSize,
        sizeof(SpillRecordHeader));
  }
  if (header.rawSize != expectedRawSize) {
    BOLT_FAIL(
        "BM spill record raw size mismatch, raw_size={}, expected={}",
        header.rawSize,
        expectedRawSize);
  }
  if (!isSupportedKind(header.compressionKind)) {
    BOLT_FAIL(
        "BM spill record has unsupported compression kind={}",
        header.compressionKind);
  }
  if (header.headerSize > size ||
      header.storedSize > size - header.headerSize) {
    BOLT_FAIL(
        "BM spill record payload exceeds record size, header_size={}, stored_size={}, record_size={}",
        header.headerSize,
        header.storedSize,
        size);
  }
  return header;
}

} // namespace bytedance::bolt::memory::bm::compress
