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

#include "bolt/shuffle/sparksql/compression/ZstdCodec.h"
#include "bolt/common/base/Exceptions.h"

#include <fmt/format.h>
#include <zstd.h>

namespace bytedance::bolt::shuffle::sparksql {

ZstdCodec::ZstdCodec(const CodecOptions& options)
    : Codec(CodecType::ZSTD, options), cctx_(nullptr), dctx_(nullptr) {
  cctx_ = ZSTD_createCCtx();
  BOLT_CODEC_CHECK(cctx_ != nullptr, "ZSTD_createCCtx failed.");

  dctx_ = ZSTD_createDCtx();
  BOLT_CODEC_CHECK(dctx_ != nullptr, "ZSTD_createDCtx failed.");

  // Set compression level
  size_t ret = ZSTD_CCtx_setParameter(
      cctx_, ZSTD_c_compressionLevel, compressionLevel());
  BOLT_CODEC_CHECK(
      !ZSTD_isError(ret),
      "ZSTD set compression level failed: {}",
      ZSTD_getErrorName(ret));

  // Enable checksum if requested
  if (checksumEnabled()) {
    ret = ZSTD_CCtx_setParameter(cctx_, ZSTD_c_checksumFlag, 1);
    BOLT_CODEC_CHECK(
        !ZSTD_isError(ret),
        "ZSTD set checksum flag failed: {}",
        ZSTD_getErrorName(ret));
  }
}

ZstdCodec::ZstdCodec(const ZstdCodecOptions& options)
    : ZstdCodec(static_cast<CodecOptions>(options)) {
  // Set number of workers if requested
  if (options.nbWorkers > 0) {
    nbWorkers_ = options.nbWorkers;
    auto ret =
        ZSTD_CCtx_setParameter(cctx_, ZSTD_c_nbWorkers, options.nbWorkers);
    BOLT_CODEC_CHECK(
        !ZSTD_isError(ret),
        "Bolt shuffle codec: ZSTD set number of workers failed: {}",
        ZSTD_getErrorName(ret));
  }
}

std::string ZstdCodec::toString() const {
  if (nbWorkers_ > 0) {
    return fmt::format("{}: {}, nbWorkers: {}", name(), nbWorkers_);
  }
  return Codec::toString();
}

ZstdCodec::~ZstdCodec() {
  if (cctx_ != nullptr) {
    ZSTD_freeCCtx(cctx_);
    cctx_ = nullptr;
  }
  if (dctx_ != nullptr) {
    ZSTD_freeDCtx(dctx_);
    dctx_ = nullptr;
  }
}

int64_t ZstdCodec::compress(
    const uint8_t* input,
    int64_t inputLength,
    uint8_t* output,
    int64_t outputLength) {
  size_t ret = ZSTD_compress2(
      cctx_,
      output,
      static_cast<size_t>(outputLength),
      input,
      static_cast<size_t>(inputLength));

  BOLT_CODEC_CHECK(
      !ZSTD_isError(ret),
      "Bolt shuffle codec: ZSTD compression failed: {}",
      ZSTD_getErrorName(ret));

  return static_cast<int64_t>(ret);
}

int64_t ZstdCodec::decompress(
    const uint8_t* input,
    int64_t inputLength,
    uint8_t* output,
    int64_t outputLength) {
  size_t ret = ZSTD_decompressDCtx(
      dctx_,
      output,
      static_cast<size_t>(outputLength),
      input,
      static_cast<size_t>(inputLength));

  BOLT_CODEC_CHECK(
      !ZSTD_isError(ret),
      "Bolt shuffle codec: ZSTD decompression failed: {}",
      ZSTD_getErrorName(ret));

  return static_cast<int64_t>(ret);
}

int64_t ZstdCodec::maxCompressedLen(int64_t inputLength) const {
  return static_cast<int64_t>(
      ZSTD_compressBound(static_cast<size_t>(inputLength)));
}

} // namespace bytedance::bolt::shuffle::sparksql
