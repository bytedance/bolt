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

#pragma once

#include <arrow/util/compression.h>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>
#include "bolt/shuffle/sparksql/compression/Compression.h"

namespace bytedance::bolt::shuffle::sparksql {

enum class CodecType { UNCOMPRESSED, GZIP, SNAPPY, LZ4, LZ4_FRAME, ZSTD };

constexpr int32_t kDefaultCompressionLevel =
    std::numeric_limits<int32_t>::max();

struct CodecOptions {
  CodecBackend backend;
  int32_t compression_level = kDefaultCompressionLevel;
  bool checksumEnabled = false;
};

class Codec {
 public:
  explicit Codec(const CodecOptions& options);
  virtual ~Codec() = default;
  static std::unique_ptr<Codec> create(
      CodecType type,
      const CodecOptions& options);

  /*
   * Compresses the input data using the specified codec options.
   *
   * @param input The input data to be compressed.
   * @param inputLength The length of the input data.
   * @param output The buffer to store the compressed data.
   * @param outputLength The length of the output buffer.
   * @return The length of the compressed data.
   */
  virtual int64_t compress(
      const uint8_t* input,
      int64_t inputLenth,
      uint8_t* output,
      int64_t outputLength) = 0;

  /*
   * Decompresses the input data using the specified codec options.
   *
   * @param input The input data to be decompressed.
   * @param inputLength The length of the input data.
   * @param output The buffer to store the decompressed data.
   * @param outputLength The length of the output buffer.
   * @return The length of the decompressed data.
   */
  virtual int64_t decompress(
      const uint8_t* input,
      int64_t inputLenth,
      uint8_t* output,
      int64_t outputLength) = 0;

  /*
   * Returns the maximum compressed length for a given input length.
   * This is useful for pre-allocating the output buffer.
   *
   * @param inputLength The length of the input data.
   * @return The maximum compressed length.
   */
  virtual int64_t maxCompressedLen(int64_t inputLength) const = 0;

 protected:
  CodecOptions options_;
};

std::unique_ptr<Codec> createCodec(
    arrow::Compression::type type,
    const CodecOptions& options);

} // namespace bytedance::bolt::shuffle::sparksql
