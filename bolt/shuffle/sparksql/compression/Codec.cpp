/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"
#include "bolt/shuffle/sparksql/compression/GzipCodec.h"
#include "bolt/shuffle/sparksql/compression/Lz4Codec.h"
#include "bolt/shuffle/sparksql/compression/SnappyCodec.h"
#include "bolt/shuffle/sparksql/compression/ZstdCodec.h"

#include <arrow/util/type_fwd.h>

#include <cmath>
#include <cstring>
namespace bytedance::bolt::shuffle::sparksql {
CodecType fromArrowCodecType(arrow::Compression::type type) {
  switch (type) {
    case arrow::Compression::type::UNCOMPRESSED:
      return CodecType::UNCOMPRESSED;
    case arrow::Compression::type::GZIP:
      return CodecType::GZIP;
    case arrow::Compression::type::SNAPPY:
      return CodecType::SNAPPY;
    case arrow::Compression::type::LZ4:
      return CodecType::LZ4;
    case arrow::Compression::type::LZ4_FRAME:
      return CodecType::LZ4_FRAME;
    case arrow::Compression::type::ZSTD:
      return CodecType::ZSTD;
    default:
      BOLT_UNSUPPORTED(
          "Bolt shuffle does not support codec type: " +
          arrow::util::Codec::GetCodecAsString(type));
  }
}

std::string CodecOptions::toString() const {
  if (compressionLevel == kDefaultCompressionLevel) {
    return fmt::format(
        "CodecOptions{{backend={}, level=default, checksum={}}}",
        getCodecBackendName(backend),
        checksumEnabled);
  }
  return fmt::format(
      "CodecOptions{{backend={}, level={}, checksum={}}}",
      getCodecBackendName(backend),
      compressionLevel,
      checksumEnabled);
}

class UncompressionCodec : public Codec {
 public:
  explicit UncompressionCodec(const CodecOptions& options)
      : Codec(CodecType::UNCOMPRESSED, options) {}

  int32_t defaultCompressionLevel() const override {
    return kDefaultCompressionLevel;
  }

  int64_t compress(
      const uint8_t* input,
      int64_t inputLength,
      uint8_t* output,
      int64_t outputLength) override {
    BOLT_CODEC_CHECK(
        outputLength >= inputLength,
        "Output length is smaller than input length");
    simd::memcpy(output, input, inputLength);
    return inputLength;
  }

  int64_t decompress(
      const uint8_t* input,
      int64_t inputLength,
      uint8_t* output,
      int64_t outputLength) override {
    BOLT_CODEC_CHECK(
        outputLength >= inputLength,
        "Output length is smaller than input length");
    simd::memcpy(output, input, inputLength);
    return inputLength;
  }

  int64_t maxCompressedLen(int64_t inputLength) const override {
    // For uncompressed codec, max compressed length equals input length
    return inputLength;
  }
};

Codec::Codec(CodecType codecType, const CodecOptions& options)
    : codecType_(codecType), options_(options) {}

std::unique_ptr<Codec> Codec::create(
    CodecType type,
    const CodecOptions& options) {
  switch (type) {
    case CodecType::UNCOMPRESSED:
      return std::make_unique<UncompressionCodec>(options);
    case CodecType::GZIP:
      return std::make_unique<GzipCodec>(options);
    case CodecType::SNAPPY:
      return std::make_unique<SnappyCodec>(options);
    case CodecType::LZ4:
      return std::make_unique<Lz4Codec>(options);
    case CodecType::LZ4_FRAME:
      return std::make_unique<Lz4FrameCodec>(options);
    case CodecType::ZSTD:
      return std::make_unique<ZstdCodec>(options);
    default:
      BOLT_CHECK(
          false, "Bolt codec: Unknown codec type: {}", static_cast<int>(type));
  }
}

std::string Codec::toString() const {
  return fmt::format("{}: {}", name(), options_.toString());
}

std::unique_ptr<Codec> createCodec(
    arrow::Compression::type type,
    const CodecOptions& options) {
  return Codec::create(fromArrowCodecType(type), options);
}

} // namespace bytedance::bolt::shuffle::sparksql
