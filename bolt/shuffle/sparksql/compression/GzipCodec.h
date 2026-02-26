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

#include "bolt/shuffle/sparksql/compression/Codec.h"

#include <zlib.h>

#include <cstdint>

namespace bytedance::bolt::shuffle::sparksql {

// Default compression level for GZIP (Z_BEST_COMPRESSION = 9)
constexpr int kGzipDefaultCompressionLevel = Z_BEST_COMPRESSION;

class GzipCodec : public Codec {
 public:
  explicit GzipCodec(const CodecOptions& options);
  ~GzipCodec() override;

  int32_t defaultCompressionLevel() const override {
    return kGzipDefaultCompressionLevel;
  }

  int64_t compress(
      const uint8_t* input,
      int64_t inputLength,
      uint8_t* output,
      int64_t outputLength) override;

  int64_t decompress(
      const uint8_t* input,
      int64_t inputLength,
      uint8_t* output,
      int64_t outputLength) override;

  int64_t maxCompressedLen(int64_t inputLength) const override;

 private:
  void initCompressor();
  void initDecompressor();
  void endCompressor();
  void endDecompressor();

  z_stream compressStream_;
  z_stream decompressStream_;
  bool compressorInitialized_;
  bool decompressorInitialized_;
};

} // namespace bytedance::bolt::shuffle::sparksql
