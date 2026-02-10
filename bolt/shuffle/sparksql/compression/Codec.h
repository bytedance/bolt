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
#include "bolt/shuffle/sparksql/compression/Compression.h"

namespace bytedance::bolt::shuffle::sparksql {

enum class CodecType { UNCOMPRESSED, GZIP, SNAPPY, LZ4, LZ4_FRAME, ZSTD };

constexpr int32_t kDefaultCompressionLevel =
    std::numeric_limits<int32_t>::max();

/// Options for codec creation.
/// Note on checksumEnabled support:
/// - ZSTD: Fully supported. Uses ZSTD_c_checksumFlag for native checksum.
/// - LZ4_FRAME: Fully supported. Uses LZ4F contentChecksumFlag.
/// - GZIP: Ignored. GZIP has built-in CRC32 checksum (always enabled per spec).
/// - LZ4 (raw): Not supported. Throws error if enabled.
/// - SNAPPY: Not supported. Throws error if enabled.
struct CodecOptions {
  CodecBackend backend;
  int32_t compressionLevel = kDefaultCompressionLevel;
  bool checksumEnabled = false;

  std::string toString() const;
};

#define BOLT_CODEC_CHECK(expr, msg, ...) \
  BOLT_CHECK(                            \
      expr,                              \
      fmt::format(                       \
          "Failed in {}: {}", toString(), detail::errorMessage(__VA_ARGS__)))

class Codec {
 public:
  explicit Codec(CodecType type, const CodecOptions& options);
  virtual ~Codec() = default;
  static std::unique_ptr<Codec> create(
      CodecType type,
      const CodecOptions& options);

  /*
   * Returns the name of the codec type. Use constexpr to maximize compile-time
   * optimization.
   */
  static constexpr std::string_view codecTypeName(CodecType type) {
    switch (type) {
      case CodecType::UNCOMPRESSED:
        return "UNCOMPRESSED";
      case CodecType::ZSTD:
        return "ZSTD";
      case CodecType::LZ4_FRAME:
        return "LZ4_FRAME";
      case CodecType::GZIP:
        return "GZIP";
      case CodecType::LZ4:
        return "LZ4";
      case CodecType::SNAPPY:
        return "SNAPPY";
      default:
        return "UNKNOWN";
    }
  }

  /*
   * Compresses the input data using the specified codec options.
   *
   * @param input The input data to be compressed.
   * @param inputLength The length of the input data.
   * @param output The buffer to store the compressed data.
   * @param outputLength The length of the output buffer.
   * @return The length of the compressed data.
   *
   * Note: when compressing failed, it would throw a BoltRuntimeError.
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
   *
   * Note: when decompressing failed, it would throw a BoltRuntimeError.
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

  /*
   * Returns the default compression level for the codec.
   *
   * @return The default compression level.
   */
  virtual int32_t defaultCompressionLevel() const = 0;

  /*
   * Returns the name of the codec.
   *
   * @return The name of the codec.
   */
  std::string name() const {
    return std::string(codecTypeName(codecType_));
  }

  /*
   * Returns a string representation of the codec, including the codec options.
   *
   * @return The string representation of the codec.
   */
  virtual std::string toString() const;

 protected:
  int32_t compressionLevel() const {
    return options_.compressionLevel == kDefaultCompressionLevel
        ? defaultCompressionLevel()
        : options_.compressionLevel;
  }

  bool checksumEnabled() const {
    return options_.checksumEnabled;
  }

 private:
  CodecType codecType_;
  CodecOptions options_;
};

std::unique_ptr<Codec> createCodec(
    arrow::Compression::type type,
    const CodecOptions& options);

} // namespace bytedance::bolt::shuffle::sparksql
