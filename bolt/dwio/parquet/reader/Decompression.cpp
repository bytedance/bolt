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

#include "bolt/dwio/parquet/reader/Decompression.h"

#include "bolt/common/compression/LzoDecompressor.h"
#include "bolt/dwio/common/BufferUtil.h"
#include "bolt/dwio/common/IntCodecCommon.h"
#include "bolt/dwio/common/SeekableInputStream.h"
#include "bolt/dwio/common/compression/Compression.h"
#include "bolt/dwio/parquet/reader/PageReader.h"
#include "bolt/dwio/parquet/reader/ParquetReaderUtil.h"
#include "bolt/dwio/parquet/thrift/FmtParquetFormatters.h"

#include <folly/ScopeGuard.h>
#include <lz4.h>
#include <zstd.h>

namespace bytedance::bolt::parquet {

static inline std::uint32_t reverse_bytes(std::uint32_t i) {
  return (i & 0xff000000u) >> 24 | (i & 0x00ff0000u) >> 8 |
      (i & 0x0000ff00u) << 8 | (i & 0x000000ffu) << 24;
}

char* FOLLY_NONNULL ensurePrefixOutputCapacity(
    BufferPtr& buffer,
    uint32_t required,
    uint32_t uncompressedSize,
    uint32_t& outputSize) {
  if (required > outputSize) {
    auto nextSize = std::min<uint32_t>(
        uncompressedSize,
        std::max<uint32_t>(required, std::max<uint32_t>(outputSize * 2, 4096)));
    AlignedBuffer::reallocate<char>(&buffer, nextSize);
    outputSize = buffer->capacity();
  }
  return buffer->asMutable<char>();
}

bool tryDecompressZstdFramePrefix(
    const char* compressedData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    uint32_t outputQuantum,
    uint32_t& outputSize,
    const std::function<bool(const char*, uint32_t)>& isPrefixReady,
    const char*& prefixData) {
  ZSTD_DStream* stream = ZSTD_createDStream();
  if (stream == nullptr) {
    return false;
  }
  auto streamGuard = folly::makeGuard([&]() { ZSTD_freeDStream(stream); });
  auto ret = ZSTD_initDStream(stream);
  if (ZSTD_isError(ret)) {
    return false;
  }

  auto* output = decompressedData->asMutable<char>();
  uint32_t produced = 0;
  ZSTD_inBuffer input{compressedData, static_cast<size_t>(compressedSize), 0};
  while (input.pos < input.size) {
    const auto nextOutputEnd =
        std::min<uint32_t>(uncompressedSize, produced + outputQuantum);
    output = ensurePrefixOutputCapacity(
        decompressedData, nextOutputEnd, uncompressedSize, outputSize);
    ZSTD_outBuffer out{
        output + produced, static_cast<size_t>(nextOutputEnd - produced), 0};
    ret = ZSTD_decompressStream(stream, &out, &input);
    if (ZSTD_isError(ret)) {
      return false;
    }
    produced += out.pos;
    if (isPrefixReady(output, produced)) {
      prefixData = output;
      return true;
    }
    if (produced >= uncompressedSize || out.pos == 0) {
      break;
    }
  }
  return false;
}

bool tryDecompressZstdBlockPrefix(
    const char* compressedData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    uint32_t& outputSize,
    const std::function<bool(const char*, uint32_t)>& isPrefixReady,
    const char*& prefixData) {
  auto* output = decompressedData->asMutable<char>();
  uint32_t produced = 0;
  uint32_t inputOffset = 0;
  uint32_t cumulativeUncompressedBlockLength = 0;
  while (produced < uncompressedSize) {
    if (produced == cumulativeUncompressedBlockLength) {
      if (inputOffset + sizeof(uint32_t) > compressedSize) {
        return false;
      }
      cumulativeUncompressedBlockLength += folly::Endian::big(
          folly::loadUnaligned<uint32_t>(compressedData + inputOffset));
      inputOffset += sizeof(uint32_t);
    }
    if (inputOffset + sizeof(uint32_t) > compressedSize) {
      return false;
    }
    const auto compressedChunkLength = folly::Endian::big(
        folly::loadUnaligned<uint32_t>(compressedData + inputOffset));
    inputOffset += sizeof(uint32_t);
    if (inputOffset + compressedChunkLength > compressedSize) {
      return false;
    }

    output = ensurePrefixOutputCapacity(
        decompressedData,
        cumulativeUncompressedBlockLength,
        uncompressedSize,
        outputSize);
    const auto decompressedSize = ZSTD_decompress(
        output + produced,
        outputSize - produced,
        compressedData + inputOffset,
        compressedChunkLength);
    if (ZSTD_isError(decompressedSize)) {
      return false;
    }
    produced += decompressedSize;
    inputOffset += compressedChunkLength;
    if (isPrefixReady(output, produced)) {
      prefixData = output;
      return true;
    }
  }
  return false;
}

const char* FOLLY_NONNULL decompressLz4AndLzo(
    const char* compressedData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    memory::MemoryPool& pool,
    const thrift::CompressionCodec::type codec_) {
  dwio::common::ensureCapacity<char>(decompressedData, uncompressedSize, &pool);

  uint32_t decompressedTotalLength = 0;
  auto* inputPtr = compressedData;
  auto* outPtr = decompressedData->asMutable<char>();
  uint32_t inputLength = compressedSize;

  while (inputLength > 0) {
    if (inputLength < sizeof(uint32_t)) {
      BOLT_FAIL(
          "{} decompression failed, input len is to small: {}",
          codec_,
          inputLength)
    }
    uint32_t decompressedBlockLength =
        folly::Endian::big(folly::loadUnaligned<uint32_t>(inputPtr));
    inputPtr += dwio::common::INT_BYTE_SIZE;
    inputLength -= dwio::common::INT_BYTE_SIZE;
    uint32_t remainingOutputSize = uncompressedSize - decompressedTotalLength;
    if (remainingOutputSize < decompressedBlockLength) {
      BOLT_FAIL(
          "{} decompression failed, remainingOutputSize is less then "
          "decompressedBlockLength, remainingOutputSize: {}, "
          "decompressedBlockLength: {}",
          remainingOutputSize,
          decompressedBlockLength)
    }
    if (inputLength <= 0) {
      break;
    }

    do {
      // Check that input length should not be negative.
      if (inputLength < sizeof(uint32_t)) {
        BOLT_FAIL(
            "{} decompression failed, input len is to small: {}",
            codec_,
            inputLength)
      }
      // Read the length of the next lz4/lzo compressed block.
      uint32_t compressedLength =
          folly::Endian::big(folly::loadUnaligned<uint32_t>(inputPtr));
      inputPtr += dwio::common::INT_BYTE_SIZE;
      inputLength -= dwio::common::INT_BYTE_SIZE;

      if (compressedLength == 0) {
        continue;
      }

      if (compressedLength > inputLength) {
        BOLT_FAIL(
            "{} decompression failed, compressedLength is less then inputLength, "
            "compressedLength: {}, inputLength: {}",
            compressedLength,
            inputLength)
      }

      // Decompress this block.
      remainingOutputSize = uncompressedSize - decompressedTotalLength;
      uint64_t decompressedSize = -1;
      if (codec_ == thrift::CompressionCodec::LZ4) {
        decompressedSize = LZ4_decompress_safe(
            inputPtr,
            outPtr,
            static_cast<int32_t>(compressedLength),
            static_cast<int32_t>(remainingOutputSize));
      } else if (codec_ == thrift::CompressionCodec::LZO) {
        decompressedSize = common::compression::lzoDecompress(
            inputPtr,
            inputPtr + compressedLength,
            outPtr,
            outPtr + remainingOutputSize);
      } else {
        BOLT_FAIL("Unsupported Parquet compression type '{}'", codec_);
      }

      BOLT_CHECK_LE(decompressedSize, remainingOutputSize);

      outPtr += decompressedSize;
      inputPtr += compressedLength;
      inputLength -= compressedLength;
      decompressedBlockLength -= decompressedSize;
      decompressedTotalLength += decompressedSize;
    } while (decompressedBlockLength > 0);
  }

  BOLT_CHECK_EQ(decompressedTotalLength, uncompressedSize);

  return decompressedData->as<char>();
}

// Ref:https://parquet.apache.org/docs/file-format/data-pages/compression/
// Parquet file supports multiple compression codecs, including:
// Uncompressed, snappy, gzip, lzo, brotli, lz4, zstd, lz4_raw.
// Function 'thriftCodecToCompressionKind' will throw exception for brotli.
const char* FOLLY_NONNULL bdCodecDecompression(
    const char* pageData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    memory::MemoryPool& pool,
    const thrift::CompressionCodec::type codec) {
  std::unique_ptr<dwio::common::SeekableInputStream> inputStream =
      std::make_unique<dwio::common::SeekableArrayInputStream>(
          pageData, compressedSize, 0);
  auto streamDebugInfo =
      fmt::format("Page Reader: Stream {}", inputStream->getName());
  std::unique_ptr<dwio::common::SeekableInputStream> decompressedStream =
      dwio::common::compression::createDecompressor(
          thriftCodecToCompressionKind(codec),
          std::move(inputStream),
          uncompressedSize,
          pool,
          getParquetDecompressionOptions(thriftCodecToCompressionKind(codec)),
          streamDebugInfo,
          nullptr,
          true,
          compressedSize);

  decompressedStream->readFully(
      decompressedData->asMutable<char>(), uncompressedSize);

  return decompressedData->as<char>();
}

const char* FOLLY_NONNULL bdZstdDecompression(
    const char* pageData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    memory::MemoryPool& pool,
    const thrift::CompressionCodec::type codec) {
  BOLT_CHECK_GT(compressedSize, 4, "Not enough input bytes");
  int32_t zstd_type = *(int32_t*)(pageData);
  if (zstd_type == ZSTD_MAGICNUMBER) {
    auto ret = ZSTD_decompress(
        decompressedData->asMutable<char>(),
        uncompressedSize,
        pageData,
        compressedSize);
    DECOMPRESSION_ENSURE(
        !ZSTD_isError(ret),
        "ZSTD returned an error: {}",
        ZSTD_getErrorName(ret));
    BOLT_CHECK_EQ(ret, uncompressedSize);
  } else {
    uint32_t totalDecompressedCount = 0;
    uint32_t outputOffset = 0;
    uint32_t inputOffset = 0;
    uint32_t cumulativeUncompressedBlockLength = 0;
    auto output = decompressedData->asMutable<char>();

    while (totalDecompressedCount < uncompressedSize) {
      if (totalDecompressedCount == cumulativeUncompressedBlockLength) {
        cumulativeUncompressedBlockLength +=
            reverse_bytes(*(uint32_t*)(pageData + inputOffset));
        inputOffset += sizeof(uint32_t);
      }
      auto compressedChunkLength =
          reverse_bytes(*(uint32_t*)(pageData + inputOffset));
      inputOffset += sizeof(uint32_t);
      auto decompressionSize = ZSTD_decompress(
          output + outputOffset,
          uncompressedSize - outputOffset,
          pageData + inputOffset,
          compressedChunkLength);
      DECOMPRESSION_ENSURE(
          !ZSTD_isError(decompressionSize),
          "ZSTD returned an error: ",
          ZSTD_getErrorName(decompressionSize));
      totalDecompressedCount += decompressionSize;
      outputOffset += decompressionSize;
      inputOffset += compressedChunkLength;
    }
    BOLT_CHECK_EQ(outputOffset, uncompressedSize);
  }
  return decompressedData->as<char>();
}

bool tryDecompressZstdPrefix(
    const char* compressedData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    uint32_t outputQuantum,
    memory::MemoryPool& pool,
    const std::function<bool(const char* data, uint32_t availableSize)>&
        isPrefixReady,
    const char*& prefixData) {
  BOLT_CHECK_GT(compressedSize, 4, "Not enough input bytes");
  dwio::common::ensureCapacity<char>(
      decompressedData,
      std::min<uint32_t>(uncompressedSize, outputQuantum),
      &pool);
  auto outputSize = static_cast<uint32_t>(decompressedData->capacity());

  // Handle both regular ZSTD frames and the Hadoop-style block framing used by
  // some Parquet writers.
  if (folly::loadUnaligned<uint32_t>(compressedData) == ZSTD_MAGICNUMBER) {
    return tryDecompressZstdFramePrefix(
        compressedData,
        decompressedData,
        compressedSize,
        uncompressedSize,
        outputQuantum,
        outputSize,
        isPrefixReady,
        prefixData);
  }
  return tryDecompressZstdBlockPrefix(
      compressedData,
      decompressedData,
      compressedSize,
      uncompressedSize,
      outputSize,
      isPrefixReady,
      prefixData);
}
} // namespace bytedance::bolt::parquet
