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

#include "bolt/shuffle/sparksql/compression/Lz4Codec.h"
#include <lz4.h>
#include <lz4hc.h>
#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::shuffle::sparksql {
Lz4Codec::Lz4Codec(const CodecOptions& options)
    : Codec(CodecType::LZ4, options) {
  BOLT_CODEC_CHECK(
      !options.checksumEnabled, "Lz4 codec does not support checksum");
}

int64_t Lz4Codec::compress(
    const uint8_t* input,
    int64_t inputLenth,
    uint8_t* output,
    int64_t outputLength) {
  if (inputLenth == 0) {
    return 0;
  }
  int64_t outputSize = 0;
  if (compressionLevel() < LZ4HC_CLEVEL_MIN) {
    outputSize = LZ4_compress_default(
        reinterpret_cast<const char*>(input),
        reinterpret_cast<char*>(output),
        static_cast<int>(inputLenth),
        static_cast<int>(outputLength));
  } else {
    outputSize = LZ4_compress_HC(
        reinterpret_cast<const char*>(input),
        reinterpret_cast<char*>(output),
        static_cast<int>(inputLenth),
        static_cast<int>(outputLength),
        compressionLevel());
  }
  BOLT_CODEC_CHECK(outputSize > 0, "Lz4 compression failure.");
  return outputSize;
}

int64_t Lz4Codec::decompress(
    const uint8_t* input,
    int64_t inputLength,
    uint8_t* output,
    int64_t outputLength) {
  if (inputLength == 0) {
    return 0;
  }
  int64_t decompressionSize = LZ4_decompress_safe(
      reinterpret_cast<const char*>(input),
      reinterpret_cast<char*>(output),
      static_cast<int>(inputLength),
      static_cast<int>(outputLength));
  BOLT_CODEC_CHECK(decompressionSize >= 0, "Lz4 decompression failure.");
  return decompressionSize;
}

int64_t Lz4Codec::maxCompressedLen(int64_t inputLength) const {
  return LZ4_compressBound(static_cast<int>(inputLength));
}

LZ4F_preferences_t getLZ4FPreferences(
    int32_t compressionLevel,
    bool checksumEnabled) {
  LZ4F_preferences_t prefs;
  memset(&prefs, 0, sizeof(prefs));
  prefs.compressionLevel = compressionLevel;
  if (checksumEnabled) {
    prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
  }
  return prefs;
};

Lz4FrameCodec::Lz4FrameCodec(const CodecOptions& options)
    : Codec(CodecType::LZ4_FRAME, options),
      prefs_(getLZ4FPreferences(compressionLevel(), checksumEnabled())) {}

int64_t Lz4FrameCodec::compress(
    const uint8_t* input,
    int64_t inputLength,
    uint8_t* output,
    int64_t outputLength) {
  if (inputLength == 0) {
    return 0;
  }
  auto totalBytesWriten = LZ4F_compressFrame(
      output,
      static_cast<size_t>(outputLength),
      input,
      static_cast<size_t>(inputLength),
      &prefs_);

  BOLT_CODEC_CHECK(!LZ4F_isError(totalBytesWriten), "Lz4 compression failure.");
  return static_cast<int64_t>(totalBytesWriten);
}

int64_t Lz4FrameCodec::decompress(
    const uint8_t* input,
    int64_t inputLength,
    uint8_t* output,
    int64_t outputLength) {
  if (inputLength == 0) {
    return 0;
  }
  LZ4F_decompressionContext_t ctx;
  size_t ret = LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION);
  BOLT_CHECK(!LZ4F_isError(ret), "Bolt shuffle codec: LZ4 init failed.");

  int64_t totalBytesWriten = 0;
  bool finished = false;
  while (!finished && inputLength != 0) {
    auto restInputSize = static_cast<size_t>(inputLength);
    auto restOutputSize = static_cast<size_t>(outputLength);

    ret = LZ4F_decompress(
        ctx,
        output,
        &restOutputSize,
        input,
        &restInputSize,
        nullptr /* options */);
    BOLT_CODEC_CHECK(!LZ4F_isError(ret), "LZ4 decompress failed.");
    finished = (ret == 0);
    BOLT_CODEC_CHECK(
        (restInputSize != 0 || restOutputSize != 0),
        "Lz4 decompression buffer too small.");
    input += restInputSize;
    inputLength -= restInputSize;
    output += restOutputSize;
    outputLength -= restOutputSize;
    totalBytesWriten += restOutputSize;
  }
  BOLT_CODEC_CHECK(
      finished, "Lz4 compressed input contains less than one frame");
  BOLT_CODEC_CHECK(
      inputLength == 0, "Lz4 compressed input contains more than one frame");
  LZ4F_freeDecompressionContext(ctx);
  return totalBytesWriten;
}

int64_t Lz4FrameCodec::maxCompressedLen(int64_t inputLength) const {
  return static_cast<int64_t>(
      LZ4F_compressFrameBound(static_cast<size_t>(inputLength), &prefs_));
}

} // namespace bytedance::bolt::shuffle::sparksql
