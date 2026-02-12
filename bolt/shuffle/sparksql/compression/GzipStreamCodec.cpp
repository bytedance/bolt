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

#include "bolt/shuffle/sparksql/compression/GzipStreamCodec.h"
#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <cstring>
#include <limits>

namespace bytedance::bolt::shuffle::sparksql {

namespace {
// GZIP format flag: add to windowBits for deflateInit2/inflateInit2
// See zlib manual: windowBits can be 8..15, +16 for GZIP, +32 for auto-detect
constexpr int kGzipFormatFlag = 16;
constexpr int kAutoDetectFlag = 32;
// Default buffer size recommendation
constexpr int64_t kDefaultBufferSize = 64 * 1024;
} // namespace

GzipStreamCompressor::GzipStreamCompressor(const CodecOptions& options)
    : StreamCompressor(CodecType::GZIP, options) {
  init();
}

GzipStreamCompressor::~GzipStreamCompressor() {
  if (initialized_) {
    deflateEnd(&stream_);
  }
}

void GzipStreamCompressor::init() {
  memset(&stream_, 0, sizeof(stream_));

  // Initialize for GZIP format: MAX_WBITS + 16
  int windowBits = MAX_WBITS + kGzipFormatFlag;
  int ret = deflateInit2(
      &stream_,
      compressionLevel(),
      Z_DEFLATED,
      windowBits,
      8, // memLevel: default value (1-9, 8 is default)
      Z_DEFAULT_STRATEGY);

  BOLT_CHECK(ret == Z_OK, "Bolt shuffle codec: GZIP deflateInit2 failed.");
  initialized_ = true;
}

StreamCompressResult GzipStreamCompressor::compress(
    const uint8_t* input,
    int64_t inputLen,
    uint8_t* output,
    int64_t outputLen) {
  BOLT_CHECK(
      initialized_, "Bolt shuffle codec: GZIP compressor not initialized.");

  static constexpr auto inputLimit =
      static_cast<int64_t>(std::numeric_limits<uInt>::max());

  stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
  stream_.avail_in = static_cast<uInt>(std::min(inputLen, inputLimit));
  stream_.next_out = reinterpret_cast<Bytef*>(output);
  stream_.avail_out = static_cast<uInt>(std::min(outputLen, inputLimit));

  int ret = deflate(&stream_, Z_NO_FLUSH);
  BOLT_CHECK(
      ret == Z_OK || ret == Z_BUF_ERROR,
      "Bolt shuffle codec: GZIP deflate failed.");

  return StreamCompressResult{
      inputLen - stream_.avail_in, outputLen - stream_.avail_out};
}

StreamFlushResult GzipStreamCompressor::flush(
    uint8_t* output,
    int64_t outputLen) {
  BOLT_CHECK(
      initialized_, "Bolt shuffle codec: GZIP compressor not initialized.");

  static constexpr auto inputLimit =
      static_cast<int64_t>(std::numeric_limits<uInt>::max());

  stream_.avail_in = 0;
  stream_.next_out = reinterpret_cast<Bytef*>(output);
  stream_.avail_out = static_cast<uInt>(std::min(outputLen, inputLimit));

  int ret = deflate(&stream_, Z_SYNC_FLUSH);
  BOLT_CHECK(ret == Z_OK, "Bolt shuffle codec: GZIP flush failed.");

  int64_t bytesWritten = outputLen - stream_.avail_out;
  return StreamFlushResult{bytesWritten, stream_.avail_out != 0};
}

StreamEndResult GzipStreamCompressor::end(uint8_t* output, int64_t outputLen) {
  BOLT_CHECK(
      initialized_, "Bolt shuffle codec: GZIP compressor not initialized.");

  static constexpr auto inputLimit =
      static_cast<int64_t>(std::numeric_limits<uInt>::max());

  stream_.avail_in = 0;
  stream_.next_out = reinterpret_cast<Bytef*>(output);
  stream_.avail_out = static_cast<uInt>(std::min(outputLen, inputLimit));

  int ret = deflate(&stream_, Z_FINISH);
  int64_t bytesWritten = outputLen - stream_.avail_out;

  if (ret == Z_STREAM_END) {
    // Stream finished successfully
    initialized_ = false;
    ret = deflateEnd(&stream_);
    BOLT_CHECK(ret == Z_OK, "GZIP deflateEnd failed.");
    return StreamEndResult{bytesWritten, true};
  } else {
    BOLT_CHECK(ret == Z_OK, "GZIP end failed.");
    return StreamEndResult{bytesWritten, false};
  }
}

void GzipStreamCompressor::reset() {
  if (initialized_) {
    int ret = deflateReset(&stream_);
    BOLT_CHECK(ret == Z_OK, "GZIP deflateReset failed.");
  } else {
    init();
  }
}

int64_t GzipStreamCompressor::recommendedInputSize() const {
  return kDefaultBufferSize;
}

int64_t GzipStreamCompressor::recommendedOutputSize(int64_t inputSize) const {
  // deflateBound provides an upper bound on compressed size
  if (initialized_) {
    return static_cast<int64_t>(deflateBound(
        const_cast<z_stream*>(&stream_), static_cast<uLong>(inputSize)));
  }
  // Conservative estimate: input size + 12 + 6 bytes per 16KB block
  return inputSize + 12 + ((inputSize / 16384) + 1) * 6;
}

GzipStreamDecompressor::GzipStreamDecompressor(const CodecOptions& /*options*/)
    : StreamDecompressor(CodecType::GZIP) {
  init();
}

GzipStreamDecompressor::~GzipStreamDecompressor() {
  if (initialized_) {
    inflateEnd(&stream_);
  }
}

void GzipStreamDecompressor::init() {
  memset(&stream_, 0, sizeof(stream_));
  finished_ = false;

  // Autodetect GZIP/ZLIB format: MAX_WBITS + 32
  int windowBits = MAX_WBITS + kAutoDetectFlag;
  int ret = inflateInit2(&stream_, windowBits);

  BOLT_CODEC_CHECK(ret == Z_OK, "GZIP inflateInit2 failed.");
  initialized_ = true;
}

StreamDecompressResult GzipStreamDecompressor::decompress(
    const uint8_t* input,
    int64_t inputLen,
    uint8_t* output,
    int64_t outputLen) {
  BOLT_CODEC_CHECK(initialized_, "GZIP decompressor not initialized.");

  static constexpr auto inputLimit =
      static_cast<int64_t>(std::numeric_limits<uInt>::max());

  stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
  stream_.avail_in = static_cast<uInt>(std::min(inputLen, inputLimit));
  stream_.next_out = reinterpret_cast<Bytef*>(output);
  stream_.avail_out = static_cast<uInt>(std::min(outputLen, inputLimit));

  int ret = inflate(&stream_, Z_SYNC_FLUSH);

  if (ret == Z_STREAM_END) {
    finished_ = true;
  } else if (ret == Z_BUF_ERROR) {
    // No progress was possible
    return StreamDecompressResult{0, 0, true};
  } else {
    BOLT_CODEC_CHECK(ret == Z_OK, "GZIP inflate failed.");
  }

  return StreamDecompressResult{
      inputLen - stream_.avail_in, outputLen - stream_.avail_out, false};
}

bool GzipStreamDecompressor::isFinished() const {
  return finished_;
}

void GzipStreamDecompressor::reset() {
  if (initialized_) {
    finished_ = false;
    int ret = inflateReset(&stream_);
    BOLT_CODEC_CHECK(ret == Z_OK, "GZIP inflateReset failed.");
  } else {
    init();
  }
}

int64_t GzipStreamDecompressor::recommendedInputSize() const {
  return kDefaultBufferSize;
}

} // namespace bytedance::bolt::shuffle::sparksql
