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

#include <cstdint>
#include <memory>
#include "bolt/shuffle/sparksql/compression/Codec.h"

namespace bytedance::bolt::shuffle::sparksql {

/// Result of a streaming compress operation.
struct StreamCompressResult {
  int64_t bytesRead; // Number of bytes consumed from input.
  int64_t bytesWritten; // Number of bytes written to output.
};

/// Result of a streaming flush operation.
struct StreamFlushResult {
  int64_t bytesWritten; // Number of bytes written to output.
  bool noMoreOutput; // If true, flush is complete.
};

/// Result of a streaming end operation.
struct StreamEndResult {
  int64_t bytesWritten; // Number of bytes written to output.
  bool noMoreOutput; // If true, end is complete.
};

/// Result of a streaming decompress operation.
struct StreamDecompressResult {
  int64_t bytesRead; // Number of bytes consumed from input.
  int64_t bytesWritten; // Number of bytes written to output.
  bool needMoreOutput; // If true, decompression is complete.
};

/// Streaming compressor interface.
/// Supports incremental compression with flush and end operations.
class StreamCompressor {
 public:
  StreamCompressor(CodecType codecType, const CodecOptions& options);
  virtual ~StreamCompressor() = default;

  std::string name() const {
    return std::string(Codec::codecTypeName(codecType_)) + "_COMPRESSOR";
  }

  virtual std::string toString() const;

  virtual int32_t defaultCompressionLevel() const = 0;

  /// Create a streaming compressor for the specified codec type.
  /// Only ZSTD, GZIP, and LZ4_FRAME are supported.
  /// Throws exception for unsupported types.
  static std::unique_ptr<StreamCompressor> create(
      CodecType type,
      const CodecOptions& options);

  /// Compress input data incrementally.
  /// If bytesRead is 0 on return, output buffer is too small.
  virtual StreamCompressResult compress(
      const uint8_t* input,
      int64_t inputLen,
      uint8_t* output,
      int64_t outputLen) = 0;

  /// Flush internal buffers to output until noMoreOutput is true.
  /// When noMoreOutput is true, flush is finished.
  virtual StreamFlushResult flush(uint8_t* output, int64_t outputLen) = 0;

  /// End the compression stream, writing any trailing data.
  /// If noMoreOutput is true, end is finished.
  /// After successful end(), compressor should not be used unless reset().
  virtual StreamEndResult end(uint8_t* output, int64_t outputLen) = 0;

  /// Reset the compressor to initial state for a new stream.
  virtual void reset() = 0;

  /// Recommended input buffer size for optimal performance.
  virtual int64_t recommendedInputSize() const = 0;

  /// Recommended output buffer size for given input size.
  virtual int64_t recommendedOutputSize(int64_t inputSize) const = 0;

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
  const CodecType codecType_;
  const CodecOptions options_;
};

/// Streaming decompressor interface.
/// Supports incremental decompression of compressed streams.
class StreamDecompressor {
 public:
  explicit StreamDecompressor(CodecType codecType) : codecType_(codecType) {}

  virtual ~StreamDecompressor() = default;

  std::string name() const {
    return std::string(Codec::codecTypeName(codecType_)) + "_DECOMPRESSOR";
  }

  virtual std::string toString() const {
    return name();
  }

  /// Create a streaming decompressor for the specified codec type.
  /// Only ZSTD, GZIP, and LZ4_FRAME are supported.
  /// Throws exception for unsupported types.
  static std::unique_ptr<StreamDecompressor> create(
      CodecType type,
      const CodecOptions& options);

  /// Decompress input data incrementally.
  /// If needMoreOutput is true, provide larger output buffer.
  virtual StreamDecompressResult decompress(
      const uint8_t* input,
      int64_t inputLen,
      uint8_t* output,
      int64_t outputLen) = 0;

  /// Check if the compressed stream has finished.
  /// If true, stream is definitely finished.
  /// If false, stream may or may not be finished (library-dependent).
  virtual bool isFinished() const = 0;

  /// Reset the decompressor to initial state for a new stream.
  virtual void reset() = 0;

  /// Recommended input buffer size for optimal performance.
  virtual int64_t recommendedInputSize() const = 0;

 private:
  const CodecType codecType_;
};

} // namespace bytedance::bolt::shuffle::sparksql
