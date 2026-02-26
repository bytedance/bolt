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

#include "bolt/shuffle/sparksql/compression/GzipCodec.h"
#include "bolt/common/base/Exceptions.h"

#include <zlib.h>

#include <cstring>

namespace bytedance::bolt::shuffle::sparksql {

namespace {
// GZIP format flag: add to windowBits for deflateInit2/inflateInit2
// See zlib manual: windowBits can be 8..15, +16 for GZIP, +32 for auto-detect
constexpr int kGzipFormatFlag = 16;
constexpr int kAutoDetectFlag = 32;
} // namespace

GzipCodec::GzipCodec(const CodecOptions& options)
    : Codec(CodecType::GZIP, options),
      compressorInitialized_(false),
      decompressorInitialized_(false) {}

GzipCodec::~GzipCodec() {
  endCompressor();
  endDecompressor();
}

void GzipCodec::initCompressor() {
  endDecompressor();
  memset(&compressStream_, 0, sizeof(compressStream_));

  // Initialize for GZIP format: MAX_WBITS + 16
  int windowBits = MAX_WBITS + kGzipFormatFlag;
  int ret = deflateInit2(
      &compressStream_,
      compressionLevel(),
      Z_DEFLATED,
      windowBits,
      8, // memLevel (default)
      Z_DEFAULT_STRATEGY);
  BOLT_CODEC_CHECK(ret == Z_OK, "GZIP deflateInit2 failed.");
  compressorInitialized_ = true;
}

void GzipCodec::endCompressor() {
  if (compressorInitialized_) {
    deflateEnd(&compressStream_);
    compressorInitialized_ = false;
  }
}

void GzipCodec::initDecompressor() {
  endCompressor();
  memset(&decompressStream_, 0, sizeof(decompressStream_));

  // Initialize for auto-detection of GZIP/ZLIB format: MAX_WBITS + 32
  int windowBits = MAX_WBITS + kAutoDetectFlag;
  int ret = inflateInit2(&decompressStream_, windowBits);
  BOLT_CODEC_CHECK(ret == Z_OK, "GZIP inflateInit2 failed.");
  decompressorInitialized_ = true;
}

void GzipCodec::endDecompressor() {
  if (decompressorInitialized_) {
    inflateEnd(&decompressStream_);
    decompressorInitialized_ = false;
  }
}

int64_t GzipCodec::compress(
    const uint8_t* input,
    int64_t inputLength,
    uint8_t* output,
    int64_t outputLength) {
  if (!compressorInitialized_) {
    initCompressor();
  }

  // Reset stream state for new compression
  int ret = deflateReset(&compressStream_);
  BOLT_CODEC_CHECK(ret == Z_OK, "GZIP deflateReset failed.");

  compressStream_.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
  compressStream_.avail_in = static_cast<uInt>(inputLength);
  compressStream_.next_out = reinterpret_cast<Bytef*>(output);
  compressStream_.avail_out = static_cast<uInt>(outputLength);

  ret = deflate(&compressStream_, Z_FINISH);
  BOLT_CODEC_CHECK(
      ret == Z_STREAM_END,
      "GZIP compression failed, output buffer may be too small.");

  return outputLength - compressStream_.avail_out;
}

int64_t GzipCodec::decompress(
    const uint8_t* input,
    int64_t inputLength,
    uint8_t* output,
    int64_t outputLength) {
  if (!decompressorInitialized_) {
    initDecompressor();
  }

  if (outputLength == 0) {
    // zlib does not allow nullptr output even when output length is 0
    return 0;
  }

  // Reset stream state for new decompression
  int ret = inflateReset(&decompressStream_);
  BOLT_CODEC_CHECK(ret == Z_OK, "GZIP inflateReset failed.");

  decompressStream_.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
  decompressStream_.avail_in = static_cast<uInt>(inputLength);
  decompressStream_.next_out = reinterpret_cast<Bytef*>(output);
  decompressStream_.avail_out = static_cast<uInt>(outputLength);

  ret = inflate(&decompressStream_, Z_FINISH);
  BOLT_CODEC_CHECK(ret == Z_STREAM_END, "GZIP decompression failed.");

  return decompressStream_.total_out;
}

int64_t GzipCodec::maxCompressedLen(int64_t inputLength) const {
  // Need to be in compressor mode to call deflateBound
  // Create a temporary stream for calculation
  z_stream tmpStream;
  memset(&tmpStream, 0, sizeof(tmpStream));

  int windowBits = MAX_WBITS + kGzipFormatFlag;
  int ret = deflateInit2(
      &tmpStream,
      compressionLevel(),
      Z_DEFLATED,
      windowBits,
      8, // memLevel: default value (1-9, 8 is default)
      Z_DEFAULT_STRATEGY);
  BOLT_CODEC_CHECK(ret == Z_OK, "GZIP deflateBound init failed.");

  int64_t maxLen = static_cast<int64_t>(
      deflateBound(&tmpStream, static_cast<uLong>(inputLength)));

  deflateEnd(&tmpStream);

  // Add extra space for potential zlib bugs in older versions (ARROW-3514)
  return maxLen + 12;
}

} // namespace bytedance::bolt::shuffle::sparksql
