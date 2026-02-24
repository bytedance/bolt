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

#include <zlib.h>
#include "bolt/shuffle/sparksql/compression/StreamCodec.h"

namespace bytedance::bolt::shuffle::sparksql {

/// GZIP streaming compressor implementation using zlib.
class GzipStreamCompressor : public StreamCompressor {
 public:
  explicit GzipStreamCompressor(const CodecOptions& options);
  ~GzipStreamCompressor() override;

  int32_t defaultCompressionLevel() const override {
    return Z_DEFAULT_COMPRESSION;
  }

  StreamCompressResult compress(
      const uint8_t* input,
      int64_t inputLen,
      uint8_t* output,
      int64_t outputLen) override;

  StreamFlushResult flush(uint8_t* output, int64_t outputLen) override;
  StreamEndResult end(uint8_t* output, int64_t outputLen) override;
  void reset() override;

  int64_t recommendedInputSize() const override;
  int64_t recommendedOutputSize(int64_t inputSize) const override;

 private:
  void init();

  z_stream stream_;
  int32_t compressionLevel_;
  bool initialized_{false};
};

/// GZIP streaming decompressor implementation using zlib.
class GzipStreamDecompressor : public StreamDecompressor {
 public:
  explicit GzipStreamDecompressor(const CodecOptions& options);
  ~GzipStreamDecompressor() override;

  StreamDecompressResult decompress(
      const uint8_t* input,
      int64_t inputLen,
      uint8_t* output,
      int64_t outputLen) override;

  bool isFinished() const override;
  void reset() override;
  int64_t recommendedInputSize() const override;

 private:
  void init();

  z_stream stream_;
  bool initialized_{false};
  bool finished_{false};
};

} // namespace bytedance::bolt::shuffle::sparksql
