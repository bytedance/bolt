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

#include "bolt/shuffle/sparksql/compression/Lz4FrameStreamCodec.h"
#include <cstring>
#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::shuffle::sparksql {

namespace {
constexpr int32_t kLz4DefaultCompressionLevel = 0; // LZ4 default
constexpr int64_t kLz4BlockSize64KB = 64 << 10;
constexpr int64_t kLz4BlockSize256KB = 256 << 10;
constexpr int64_t kLz4BlockSize1MB = 1 << 20;
constexpr int64_t kLz4BlockSize4MB = 4 << 20;
constexpr int64_t kDefaultBufferSize = kLz4BlockSize64KB;

LZ4F_blockSizeID_t toLz4BlockSizeId(int64_t blockSize) {
  BOLT_CHECK_GE(blockSize, 0, "LZ4 frame block size must be non-negative.");
  if (blockSize == 0) {
    return LZ4F_default;
  }
  if (blockSize <= kLz4BlockSize64KB) {
    return LZ4F_max64KB;
  }
  if (blockSize <= kLz4BlockSize256KB) {
    return LZ4F_max256KB;
  }
  if (blockSize <= kLz4BlockSize1MB) {
    return LZ4F_max1MB;
  }
  if (blockSize <= kLz4BlockSize4MB) {
    return LZ4F_max4MB;
  }
  BOLT_FAIL(
      "LZ4 frame block size {} exceeds maximum supported size ({}).",
      blockSize,
      kLz4BlockSize4MB);
}

int64_t lz4BlockSizeBytes(LZ4F_blockSizeID_t blockSizeId) {
  switch (blockSizeId) {
    case LZ4F_max256KB:
      return kLz4BlockSize256KB;
    case LZ4F_max1MB:
      return kLz4BlockSize1MB;
    case LZ4F_max4MB:
      return kLz4BlockSize4MB;
    case LZ4F_default:
    case LZ4F_max64KB:
    default:
      return kLz4BlockSize64KB;
  }
}

LZ4F_preferences_t DefaultPreferences() {
  LZ4F_preferences_t prefs;
  memset(&prefs, 0, sizeof(prefs));
  return prefs;
}

LZ4F_preferences_t PreferencesWithCompressionLevel(int compressionLevel) {
  LZ4F_preferences_t prefs = DefaultPreferences();
  prefs.compressionLevel = compressionLevel;
  return prefs;
}
} // namespace

Lz4FrameStreamCompressor::Lz4FrameStreamCompressor(const CodecOptions& options)
    : StreamCompressor(CodecType::LZ4_FRAME, options) {
  init();
}

Lz4FrameStreamCompressor::~Lz4FrameStreamCompressor() {
  if (cctx_) {
    LZ4F_freeCompressionContext(cctx_);
  }
}

void Lz4FrameStreamCompressor::init() {
  prefs_ = PreferencesWithCompressionLevel(compressionLevel());
  // Enable content checksum if requested
  if (checksumEnabled()) {
    prefs_.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
  }
  if (useIndependentBlocks()) {
    prefs_.frameInfo.blockMode = LZ4F_blockIndependent;
  }
  prefs_.frameInfo.blockSizeID = toLz4BlockSizeId(blockSize());
  frameState_ = FrameState::kNotStarted;

  LZ4F_errorCode_t ret = LZ4F_createCompressionContext(&cctx_, LZ4F_VERSION);
  BOLT_CHECK(
      !LZ4F_isError(ret),
      "Bolt shuffle codec: LZ4F_createCompressionContext failed.");
}

int64_t Lz4FrameStreamCompressor::writeHeader(
    uint8_t*& dst,
    size_t& dstCapacity) {
  BOLT_CHECK(
      frameState_ != FrameState::kEnded,
      "Bolt shuffle codec: frame has already ended; call reset() first.");
  if (frameState_ == FrameState::kInProgress) {
    return 0;
  }

  if (dstCapacity < LZ4F_HEADER_SIZE_MAX) {
    return -1; // Output too small for header
  }

  size_t ret = LZ4F_compressBegin(cctx_, dst, dstCapacity, &prefs_);
  BOLT_CHECK(
      !LZ4F_isError(ret), "Bolt shuffle codec: LZ4F_compressBegin failed.");

  frameState_ = FrameState::kInProgress;
  dst += ret;
  dstCapacity -= ret;
  return static_cast<int64_t>(ret);
}

StreamCompressResult Lz4FrameStreamCompressor::compress(
    const uint8_t* input,
    int64_t inputLen,
    uint8_t* output,
    int64_t outputLen) {
  auto dst = output;
  auto dstCapacity = static_cast<size_t>(outputLen);
  int64_t bytesWritten = 0;

  // Write header on first call
  int64_t headerBytes = writeHeader(dst, dstCapacity);
  BOLT_CHECK(
      headerBytes >= 0,
      "Bolt shuffle codec: LZ4F_compressBegin failed, output too small for header.");
  bytesWritten += headerBytes;

  // Check if output has enough space
  size_t bound = LZ4F_compressBound(static_cast<size_t>(inputLen), &prefs_);
  BOLT_CHECK(
      bound <= dstCapacity,
      "Bolt shuffle codec: LZ4F_compressUpdate failed, output too small for compression.");

  size_t ret = LZ4F_compressUpdate(
      cctx_,
      dst,
      dstCapacity,
      input,
      static_cast<size_t>(inputLen),
      nullptr /* options */);
  BOLT_CHECK(
      !LZ4F_isError(ret), "Bolt shuffle codec: LZ4F_compressUpdate failed.");

  bytesWritten += static_cast<int64_t>(ret);
  return StreamCompressResult{inputLen, bytesWritten};
}

StreamFlushResult Lz4FrameStreamCompressor::flush(
    uint8_t* output,
    int64_t outputLen) {
  auto dst = output;
  auto dstCapacity = static_cast<size_t>(outputLen);
  int64_t bytesWritten = 0;

  // Write header if not yet written
  int64_t headerBytes = writeHeader(dst, dstCapacity);
  BOLT_CHECK(
      headerBytes >= 0,
      "Bolt shuffle codec: LZ4F_compressBegin failed, output too small for header.");
  bytesWritten += headerBytes;

  // Check if output has enough space
  size_t bound = LZ4F_compressBound(0, &prefs_);
  BOLT_CHECK(
      bound <= dstCapacity,
      "Bolt shuffle codec: LZ4F_flush failed, output too small for flush.");

  size_t ret = LZ4F_flush(cctx_, dst, dstCapacity, nullptr /* options */);
  BOLT_CHECK(!LZ4F_isError(ret), "Bolt shuffle codec: LZ4F_flush failed.");

  bytesWritten += static_cast<int64_t>(ret);
  return StreamFlushResult{bytesWritten, ret == 0};
}

StreamEndResult Lz4FrameStreamCompressor::end(
    uint8_t* output,
    int64_t outputLen) {
  auto dst = output;
  auto dstCapacity = static_cast<size_t>(outputLen);
  int64_t bytesWritten = 0;

  // Write header if not yet written
  int64_t headerBytes = writeHeader(dst, dstCapacity);
  BOLT_CODEC_CHECK(
      headerBytes >= 0,
      " LZ4F_compressBegin failed, output too small for header.");
  bytesWritten += headerBytes;

  // Check if output has enough space
  size_t bound = LZ4F_compressBound(0, &prefs_);
  BOLT_CODEC_CHECK(
      bound <= dstCapacity,
      "LZ4F_compressEnd failed, output too small for end.");

  size_t ret = LZ4F_compressEnd(cctx_, dst, dstCapacity, nullptr /* options */);
  BOLT_CODEC_CHECK(!LZ4F_isError(ret), "LZ4F_compressEnd failed.");
  frameState_ = FrameState::kEnded;

  bytesWritten += static_cast<int64_t>(ret);
  return StreamEndResult{bytesWritten, true};
}

void Lz4FrameStreamCompressor::reset() {
  // A context that has not started or whose frame ended starts the next one at
  // LZ4F_compressBegin, keeping the block buffer and state it allocated:
  // recreating them for every frame dominates when frames are small and many.
  // An in-progress frame has no reset in the LZ4 frame API: free and recreate.
  if (frameState_ != FrameState::kInProgress) {
    frameState_ = FrameState::kNotStarted;
    return;
  }
  if (cctx_) {
    LZ4F_freeCompressionContext(cctx_);
    cctx_ = nullptr;
  }
  init();
}

int64_t Lz4FrameStreamCompressor::recommendedInputSize() const {
  return lz4BlockSizeBytes(prefs_.frameInfo.blockSizeID);
}

int64_t Lz4FrameStreamCompressor::recommendedOutputSize(
    int64_t inputSize) const {
  return static_cast<int64_t>(
             LZ4F_compressBound(static_cast<size_t>(inputSize), &prefs_)) +
      LZ4F_HEADER_SIZE_MAX;
}

Lz4FrameStreamDecompressor::Lz4FrameStreamDecompressor(
    const CodecOptions& /*options*/)
    : StreamDecompressor(CodecType::LZ4_FRAME) {
  init();
}

Lz4FrameStreamDecompressor::~Lz4FrameStreamDecompressor() {
  if (dctx_) {
    LZ4F_freeDecompressionContext(dctx_);
  }
}

void Lz4FrameStreamDecompressor::init() {
  finished_ = false;

  LZ4F_errorCode_t ret = LZ4F_createDecompressionContext(&dctx_, LZ4F_VERSION);
  BOLT_CODEC_CHECK(
      !LZ4F_isError(ret), "LZ4F_createDecompressionContext failed.");
}

StreamDecompressResult Lz4FrameStreamDecompressor::decompress(
    const uint8_t* input,
    int64_t inputLen,
    uint8_t* output,
    int64_t outputLen) {
  auto srcSize = static_cast<size_t>(inputLen);
  auto dstCapacity = static_cast<size_t>(outputLen);

  size_t ret = LZ4F_decompress(
      dctx_, output, &dstCapacity, input, &srcSize, nullptr /* options */);

  BOLT_CODEC_CHECK(!LZ4F_isError(ret), "LZ4F_decompress failed.");

  finished_ = (ret == 0);

  return StreamDecompressResult{
      static_cast<int64_t>(srcSize),
      static_cast<int64_t>(dstCapacity),
      srcSize == 0 && dstCapacity == 0};
}

bool Lz4FrameStreamDecompressor::isFinished() const {
  return finished_;
}

void Lz4FrameStreamDecompressor::reset() {
#if defined(LZ4_VERSION_NUMBER) && LZ4_VERSION_NUMBER >= 10800
  // LZ4F_resetDecompressionContext appeared in 1.8.0
  LZ4F_resetDecompressionContext(dctx_);
  finished_ = false;
#else
  // For older versions, free and recreate
  if (dctx_) {
    LZ4F_freeDecompressionContext(dctx_);
    dctx_ = nullptr;
  }
  init();
#endif
}

int64_t Lz4FrameStreamDecompressor::recommendedInputSize() const {
  return kDefaultBufferSize;
}

} // namespace bytedance::bolt::shuffle::sparksql
