/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/shuffle/sparksql/compression/SnappyCodec.h"
#include "bolt/common/base/Exceptions.h"

#include <snappy.h>

namespace bytedance::bolt::shuffle::sparksql {

SnappyCodec::SnappyCodec(const CodecOptions& options)
    : Codec(CodecType::SNAPPY, options) {
  BOLT_CODEC_CHECK(
      !options.checksumEnabled, "Snappy codec does not support checksum");
}

int64_t SnappyCodec::compress(
    const uint8_t* input,
    int64_t inputLength,
    uint8_t* output,
    int64_t outputLength) {
  size_t compressedSize;
  snappy::RawCompress(
      reinterpret_cast<const char*>(input),
      static_cast<size_t>(inputLength),
      reinterpret_cast<char*>(output),
      &compressedSize);

  BOLT_CODEC_CHECK(
      static_cast<int64_t>(compressedSize) <= outputLength,
      "Snappy compression output buffer too small.");

  return static_cast<int64_t>(compressedSize);
}

int64_t SnappyCodec::decompress(
    const uint8_t* input,
    int64_t inputLength,
    uint8_t* output,
    int64_t outputLength) {
  size_t uncompressedLength;
  bool success = snappy::GetUncompressedLength(
      reinterpret_cast<const char*>(input),
      static_cast<size_t>(inputLength),
      &uncompressedLength);

  BOLT_CODEC_CHECK(success, "Corrupt snappy compressed data.");

  BOLT_CODEC_CHECK(
      static_cast<int64_t>(uncompressedLength) <= outputLength,
      "Snappy decompression output buffer too small.");

  success = snappy::RawUncompress(
      reinterpret_cast<const char*>(input),
      static_cast<size_t>(inputLength),
      reinterpret_cast<char*>(output));

  BOLT_CODEC_CHECK(success, "Snappy decompression failed.");

  return static_cast<int64_t>(uncompressedLength);
}

int64_t SnappyCodec::maxCompressedLen(int64_t inputLength) const {
  return static_cast<int64_t>(
      snappy::MaxCompressedLength(static_cast<size_t>(inputLength)));
}

} // namespace bytedance::bolt::shuffle::sparksql
