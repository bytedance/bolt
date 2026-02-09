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

#include <lz4frame.h>
#include "bolt/shuffle/sparksql/compression/Lz4Codec.h"
#include "bolt/shuffle/sparksql/compression/StreamCodec.h"

namespace bytedance::bolt::shuffle::sparksql {

/// LZ4 Frame streaming compressor implementation.
class Lz4FrameStreamCompressor : public StreamCompressor {
 public:
  explicit Lz4FrameStreamCompressor(const CodecOptions& options);
  ~Lz4FrameStreamCompressor() override;

  int32_t defaultCompressionLevel() const override {
    return kLz4DefaultCompressionLevel;
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
  int64_t writeHeader(uint8_t*& dst, size_t& dstCapacity);

  LZ4F_cctx* cctx_{nullptr};
  LZ4F_preferences_t prefs_;
  bool firstTime_{true};
};

/// LZ4 Frame streaming decompressor implementation.
class Lz4FrameStreamDecompressor : public StreamDecompressor {
 public:
  explicit Lz4FrameStreamDecompressor(const CodecOptions& options);
  ~Lz4FrameStreamDecompressor() override;

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

  LZ4F_dctx* dctx_{nullptr};
  bool finished_{false};
};

} // namespace bytedance::bolt::shuffle::sparksql
