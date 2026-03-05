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

#include "bolt/shuffle/sparksql/compression/ZstdStreamCodec.h"
#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::shuffle::sparksql {

namespace {
constexpr int32_t kZstdDefaultStreamCompressionLevel = 1;
} // namespace

ZstdStreamCompressor::ZstdStreamCompressor(const CodecOptions& options)
    : StreamCompressor(CodecType::ZSTD, options) {
  init();
}

ZstdStreamCompressor::ZstdStreamCompressor(const ZstdCodecOptions& options)
    : ZstdStreamCompressor(static_cast<const CodecOptions&>(options)) {
  // Set number of workers if requested
  if (options.nbWorkers > 0) {
    auto ret =
        ZSTD_CCtx_setParameter(cstream_, ZSTD_c_nbWorkers, options.nbWorkers);
    BOLT_CODEC_CHECK(
        !ZSTD_isError(ret),
        "ZSTD set number of workers failed: %s",
        ZSTD_getErrorName(ret));
  }
}

ZstdStreamCompressor::~ZstdStreamCompressor() {
  if (cstream_) {
    ZSTD_freeCStream(cstream_);
  }
}

void ZstdStreamCompressor::init() {
  cstream_ = ZSTD_createCStream();
  BOLT_CODEC_CHECK(cstream_ != nullptr, "ZSTD_createCStream failed.");

  size_t ret = ZSTD_initCStream(cstream_, compressionLevel());
  BOLT_CODEC_CHECK(!ZSTD_isError(ret), "ZSTD_initCStream failed.");

  if (checksumEnabled()) {
    ret = ZSTD_CCtx_setParameter(cstream_, ZSTD_c_checksumFlag, 1);
    BOLT_CHECK(!ZSTD_isError(ret), "ZSTD set checksum failed.");
  }
}

StreamCompressResult ZstdStreamCompressor::compress(
    const uint8_t* input,
    int64_t inputLen,
    uint8_t* output,
    int64_t outputLen) {
  ZSTD_inBuffer inBuf{input, static_cast<size_t>(inputLen), 0};
  ZSTD_outBuffer outBuf{output, static_cast<size_t>(outputLen), 0};

  size_t ret = ZSTD_compressStream(cstream_, &outBuf, &inBuf);
  BOLT_CODEC_CHECK(!ZSTD_isError(ret), "ZSTD_compressStream failed.");

  return StreamCompressResult{
      static_cast<int64_t>(inBuf.pos), static_cast<int64_t>(outBuf.pos)};
}

StreamFlushResult ZstdStreamCompressor::flush(
    uint8_t* output,
    int64_t outputLen) {
  ZSTD_outBuffer outBuf{output, static_cast<size_t>(outputLen), 0};

  size_t ret = ZSTD_flushStream(cstream_, &outBuf);
  BOLT_CODEC_CHECK(!ZSTD_isError(ret), "ZSTD_flushStream failed.");

  // ret = 0 means flush finish
  return StreamFlushResult{static_cast<int64_t>(outBuf.pos), ret == 0};
}

StreamEndResult ZstdStreamCompressor::end(uint8_t* output, int64_t outputLen) {
  ZSTD_outBuffer outBuf{output, static_cast<size_t>(outputLen), 0};

  size_t ret = ZSTD_endStream(cstream_, &outBuf);
  BOLT_CODEC_CHECK(!ZSTD_isError(ret), "ZSTD_endStream failed.");

  // ret = 0 means end finish
  return StreamEndResult{static_cast<int64_t>(outBuf.pos), ret == 0};
}

void ZstdStreamCompressor::reset() {
  size_t ret = ZSTD_CCtx_reset(cstream_, ZSTD_reset_session_only);
  BOLT_CODEC_CHECK(!ZSTD_isError(ret), "ZSTD_CCtx_reset failed.");
}

int64_t ZstdStreamCompressor::recommendedInputSize() const {
  return static_cast<int64_t>(ZSTD_CStreamInSize());
}

int64_t ZstdStreamCompressor::recommendedOutputSize(int64_t inputSize) const {
  return static_cast<int64_t>(
      ZSTD_compressBound(static_cast<size_t>(inputSize)));
}

ZstdStreamDecompressor::ZstdStreamDecompressor(const CodecOptions& /*options*/)
    : StreamDecompressor(CodecType::ZSTD) {
  init();
}

ZstdStreamDecompressor::ZstdStreamDecompressor(const ZstdCodecOptions&)
    : StreamDecompressor(CodecType::ZSTD) {
  init();
}

ZstdStreamDecompressor::~ZstdStreamDecompressor() {
  if (dstream_) {
    ZSTD_freeDStream(dstream_);
  }
}

void ZstdStreamDecompressor::init() {
  dstream_ = ZSTD_createDStream();
  BOLT_CODEC_CHECK(dstream_ != nullptr, "ZSTD_createDStream failed.");

  size_t ret = ZSTD_initDStream(dstream_);
  BOLT_CODEC_CHECK(!ZSTD_isError(ret), "ZSTD_initDStream failed.");

  finished_ = false;
}

StreamDecompressResult ZstdStreamDecompressor::decompress(
    const uint8_t* input,
    int64_t inputLen,
    uint8_t* output,
    int64_t outputLen) {
  ZSTD_inBuffer inBuf{input, static_cast<size_t>(inputLen), 0};
  ZSTD_outBuffer outBuf{output, static_cast<size_t>(outputLen), 0};

  size_t ret = ZSTD_decompressStream(dstream_, &outBuf, &inBuf);
  BOLT_CODEC_CHECK(!ZSTD_isError(ret), "ZSTD_decompressStream failed.");

  finished_ = (ret == 0);
  bool needMoreOutput = ret > 0 && outBuf.pos == outBuf.size;

  return StreamDecompressResult{
      static_cast<int64_t>(inBuf.pos),
      static_cast<int64_t>(outBuf.pos),
      needMoreOutput};
}

bool ZstdStreamDecompressor::isFinished() const {
  return finished_;
}

void ZstdStreamDecompressor::reset() {
  size_t ret = ZSTD_DCtx_reset(dstream_, ZSTD_reset_session_only);
  BOLT_CODEC_CHECK(!ZSTD_isError(ret), "ZSTD_DCtx_reset failed.");
  finished_ = false;
}

int64_t ZstdStreamDecompressor::recommendedInputSize() const {
  return static_cast<int64_t>(ZSTD_DStreamInSize());
}

} // namespace bytedance::bolt::shuffle::sparksql
