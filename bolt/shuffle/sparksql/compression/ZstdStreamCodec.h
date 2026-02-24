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

#include <zstd.h>
#include "bolt/shuffle/sparksql/compression/StreamCodec.h"
#include "bolt/shuffle/sparksql/compression/ZstdCodec.h"

namespace bytedance::bolt::shuffle::sparksql {

/// ZSTD streaming compressor implementation.
class ZstdStreamCompressor : public StreamCompressor {
 public:
  explicit ZstdStreamCompressor(const CodecOptions& options);
  explicit ZstdStreamCompressor(const ZstdCodecOptions& options);
  ~ZstdStreamCompressor() override;

  int32_t defaultCompressionLevel() const override {
    return kZstdDefaultCompressionLevel;
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

  ZSTD_CStream* cstream_{nullptr};
};

/// ZSTD streaming decompressor implementation.
class ZstdStreamDecompressor : public StreamDecompressor {
 public:
  explicit ZstdStreamDecompressor(const CodecOptions& options);
  explicit ZstdStreamDecompressor(const ZstdCodecOptions& options);
  ~ZstdStreamDecompressor() override;

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

  ZSTD_DStream* dstream_{nullptr};
  bool finished_{false};
};

} // namespace bytedance::bolt::shuffle::sparksql
