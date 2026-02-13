/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once
#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/status.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <zstd.h>
#include <string>
#include "bolt/common/base/SimdUtil.h"
#include "bolt/common/time/Timer.h"
#include "bolt/functions/InlineFlatten.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/compression/ZstdCodec.h"
#include "bolt/shuffle/sparksql/compression/ZstdStreamCodec.h"
namespace bytedance::bolt::shuffle::sparksql {

class AdaptiveParallelZstdCodec {
 public:
  // todo: make configurable
  static constexpr int32_t kParallelCompressionThreshold = 2 * 1024 * 1024;
  static constexpr int32_t kWorkerNumber = 2;

  AdaptiveParallelZstdCodec(
      int32_t compressionLevel,
      bool compress,
      arrow::MemoryPool* pool,
      bool checksumEnabled = true)
      : zstdCompressor_(
            compress ? std::make_unique<ZstdStreamCompressor>(CodecOptions{
                           CodecBackend::NONE,
                           compressionLevel,
                           checksumEnabled})
                     : nullptr),
        parallelZstdCompressor_(
            compress ? std::make_unique<ZstdStreamCompressor>(ZstdCodecOptions{
                           CodecBackend::NONE,
                           compressionLevel,
                           checksumEnabled,
                           kWorkerNumber})
                     : nullptr),
        zstdDecompressor_(
            compress ? nullptr
                     : std::make_unique<ZstdStreamDecompressor>(CodecOptions{
                           CodecBackend::NONE,
                           compressionLevel,
                           checksumEnabled})),
        BUFFER_SIZE(compress ? ZSTD_CStreamInSize() : 0),
        MAX_COMPRESS_SIZE(
            compress ? zstdCompressor_->recommendedOutputSize(BUFFER_SIZE) * 2
                     : ZSTD_DStreamInSize()),
        pool_(pool),
        checksumEnabled_(checksumEnabled) {
    if (compress) {
      uncompressedBuffer_ =
          arrow::AllocateResizableBuffer(BUFFER_SIZE, pool_).ValueOrDie();
      uncompressBufferPtr_ = uncompressedBuffer_->mutable_data();
      compressedBuffer_ =
          arrow::AllocateResizableBuffer(MAX_COMPRESS_SIZE, pool_).ValueOrDie();
      compressBufferPtr_ = compressedBuffer_->mutable_data();
      LOG(INFO) << "ZstdStreamCodec isCompress = " << compress
                << ", cin buffer = " << BUFFER_SIZE
                << ", cout = " << MAX_COMPRESS_SIZE
                << ", compressionLevel = " << compressionLevel
                << ", checksumEnabled = " << checksumEnabled;

    } else {
      compressedBuffer_ =
          arrow::AllocateResizableBuffer(MAX_COMPRESS_SIZE, pool_).ValueOrDie();
      compressBufferPtr_ = compressedBuffer_->mutable_data();
    }
  }

  arrow::Status CompressAndFlush(
      folly::Range<uint8_t**> rows,
      arrow::io::OutputStream* outputStream,
      const int64_t rawSize,
      const RowVectorLayout layout) {
    auto& compressor = rawSize >= kParallelCompressionThreshold
        ? *parallelZstdCompressor_
        : *zstdCompressor_;
    RETURN_NOT_OK(outputStream->Write((uint8_t*)(&layout), 1));
    for (auto i = 0; i < rows.size(); ++i) {
      uint8_t* input = rows.data()[i];
      // rowSize + 4 byte header
      auto inputSize = *((int32_t*)input) + sizeof(int32_t);
      if (inputSize >= BUFFER_SIZE || inputSize + len_ > BUFFER_SIZE) {
        // compress cached data first
        if (len_) {
          RETURN_NOT_OK(CompressInternal(
              compressor,
              uncompressBufferPtr_,
              len_,
              compressBufferPtr_,
              MAX_COMPRESS_SIZE,
              outputStream));
          len_ = 0;
        }
        // input row is extremely large
        if (inputSize >= BUFFER_SIZE) {
          auto outputSize = compressor.recommendedOutputSize(inputSize) * 2;
          if (largeBuffer_) {
            RETURN_NOT_OK(largeBuffer_->Resize(outputSize));
          } else {
            ARROW_ASSIGN_OR_RAISE(
                largeBuffer_,
                arrow::AllocateResizableBuffer(outputSize, pool_));
          }
          RETURN_NOT_OK(CompressInternal(
              compressor,
              input,
              (size_t)inputSize,
              largeBuffer_->mutable_data(),
              outputSize,
              outputStream));
        } else {
          // inputSize + len_ > BUFFER_SIZE but inputSize < BUFFER_SIZE
          bytedance::bolt::simd::memcpy(
              uncompressBufferPtr_ + len_, input, inputSize);
          len_ += inputSize;
        }
      } else {
        // do not compress, cache to buffer_
        bytedance::bolt::simd::memcpy(
            uncompressBufferPtr_ + len_, input, inputSize);
        len_ += inputSize;
      }
    }
    if (len_) {
      RETURN_NOT_OK(CompressInternal(
          compressor,
          uncompressBufferPtr_,
          len_,
          compressBufferPtr_,
          MAX_COMPRESS_SIZE,
          outputStream));
      len_ = 0;
    }

    uint64_t compressTime = 0;
    uint64_t writeTime = 0;
    StreamEndResult endResult;
    do {
      bytedance::bolt::NanosecondTimer timer(&compressTime);
      endResult = compressor.end(compressBufferPtr_, MAX_COMPRESS_SIZE);
      if (endResult.bytesWritten > 0) {
        bytedance::bolt::NanosecondTimer timer1(&writeTime);
        RETURN_NOT_OK(
            outputStream->Write(compressBufferPtr_, endResult.bytesWritten));
      }
    } while (!endResult.noMoreOutput);
    compressTime_ += (compressTime - writeTime);
    writeTime_ += writeTime;
    compressor.reset();
    len_ = 0;
    return arrow::Status::OK();
  }

  arrow::Status Decompress(
      arrow::io::InputStream* inputStream,
      uint8_t* dst,
      int32_t dstSize,
      int32_t offset,
      int32_t& outputLen,
      bool& eof,
      bool& layoutEnd,
      RowVectorLayout& layout) {
    int32_t dstPos = offset;
    while (dstPos < dstSize) {
      if (needRead_) {
        ARROW_ASSIGN_OR_RAISE(
            srcSize_, inputStream->Read(MAX_COMPRESS_SIZE, compressBufferPtr_));
        srcPos_ = 0;
        if (srcSize_ == 0) {
          if (frameFinished_) [[likely]] {
            eof = true;
            break;
          } else {
            return arrow::Status::Invalid(
                "inputStream no more data, but frame is not finished, dstPos = " +
                std::to_string(dstPos) +
                ", dstSize = " + std::to_string(dstSize) +
                ", inputSize = " + std::to_string(srcSize_) +
                ", inputPos = " + std::to_string(srcPos_));
          }
        } else if (frameFinished_) {
          // first bytes is layout
          if (skipHeader(layout, dstPos, offset, outputLen, layoutEnd)) {
            // if frame changed, return
            return arrow::Status::OK();
          }
        }
        frameFinished_ = false;
      }

      auto result = zstdDecompressor_->decompress(
          compressBufferPtr_ + srcPos_,
          srcSize_ - srcPos_,
          dst + dstPos,
          dstSize - dstPos);
      srcPos_ += result.bytesRead;
      dstPos += result.bytesWritten;

      // we have completed a frame
      if (zstdDecompressor_->isFinished()) {
        frameFinished_ = true;
        // need to read from the inputStream only if source buffer are
        // consumed
        needRead_ = srcPos_ == srcSize_;
        if (!needRead_) {
          if (skipHeader(layout, dstPos, offset, outputLen, layoutEnd)) {
            needRead_ = srcPos_ == srcSize_;
            return arrow::Status::OK();
          }
        }
      } else {
        needRead_ = dstPos < dstSize;
      }
    }
    outputLen = dstPos - offset;
    layoutEnd = false;
    layout = layout_;
    return arrow::Status::OK();
  }

  const uint64_t getCompressTime() const {
    return compressTime_;
  }

  const uint64_t getWriteTime() const {
    return writeTime_;
  }

  void markHeaderSkipped(size_t sizeInMemory) {
    BOLT_DCHECK(
        sizeInMemory <= srcSize_ - srcPos_,
        "sizeInMemory " + std::to_string(sizeInMemory) + " should less than " +
            std::to_string(srcSize_ - srcPos_));
    if (sizeInMemory == 0) {
      srcSize_ = 0;
      srcPos_ = 0;
      needRead_ = true;
    } else {
      srcPos_ = srcSize_ - sizeInMemory;
      needRead_ = false;
    }
    frameFinished_ = false;
    layout_ = RowVectorLayout::kComposite;
  }

  void getReadAheadData(uint8_t** data, size_t& size) {
    if (srcPos_ < srcSize_) {
      *data = compressBufferPtr_ + srcPos_;
      size = srcSize_ - srcPos_;
    } else {
      *data = nullptr;
      size = 0;
    }
    return;
  }

  uint8_t nextLayout() const {
    return static_cast<uint8_t>(layout_);
  }

 private:
  arrow::Status CompressInternal(
      ZstdStreamCompressor& compressor,
      const uint8_t* input,
      int64_t inputLen,
      uint8_t* output,
      int64_t outputLen,
      arrow::io::OutputStream* outputStream) {
    uint64_t compressTime = 0, writeTime = 0;
    {
      bytedance::bolt::NanosecondTimer timer(&compressTime);
      StreamCompressResult result;
      auto outputPos = 0;
      do {
        result = compressor.compress(
            input, inputLen, output + outputPos, outputLen - outputPos);
        input += result.bytesRead;
        inputLen -= result.bytesRead;
        outputPos += result.bytesWritten;
        if (outputPos == outputLen || inputLen == 0) {
          // output buffer is full or input is empty, write to the output
          // stream
          bytedance::bolt::NanosecondTimer timer1(&writeTime);
          RETURN_NOT_OK(outputStream->Write(output, outputPos));
        }
      } while (inputLen > 0);
    }
    compressTime_ += (compressTime - writeTime);
    writeTime_ += writeTime;
    return arrow::Status::OK();
  }

  arrow::Status endCompressionStream(
      ZstdStreamCompressor& compressor,
      arrow::io::OutputStream* outputStream) {
    ZSTD_outBuffer outBuf{compressBufferPtr_, MAX_COMPRESS_SIZE, 0};
    ZSTD_inBuffer inBuf{uncompressBufferPtr_, len_, 0};
    uint64_t compressTime = 0, writeTime = 0;
    size_t ret = 0;
    {
      bytedance::bolt::NanosecondTimer timer(&compressTime);
      do {
        ret = ZSTD_compressStream2(currentCCtx_, &outBuf, &inBuf, ZSTD_e_end);
        if (ZSTD_isError(ret)) [[unlikely]] {
          return arrow::Status::Invalid(
              "ZSTD_compressStream2 failed by ZSTD_e_end: " +
              std::string(ZSTD_getErrorName(ret)));
        }
        if (ret > 0 && outBuf.pos == outBuf.size) {
          bytedance::bolt::NanosecondTimer timer1(&writeTime);
          RETURN_NOT_OK(outputStream->Write(outBuf.dst, outBuf.pos));
          outBuf.pos = 0;
        }
      } while (ret != 0);
    }
    compressTime_ += (compressTime - writeTime);
    writeTime_ += writeTime;
    len_ = 0;
    if (outBuf.pos) {
      bytedance::bolt::NanosecondTimer timer(&writeTime_);
      RETURN_NOT_OK(outputStream->Write(compressBufferPtr_, outBuf.pos));
    }
    return arrow::Status::OK();
  }

  arrow::Status resetCCtx() {
    auto ret = ZSTD_CCtx_reset(currentCCtx_, ZSTD_reset_session_only);
    if (ZSTD_isError(ret)) {
      return arrow::Status::Invalid("ZSTD_CCtx_reset failed");
    }
    currentCCtx_ = nullptr;
    return arrow::Status::OK();
  }

  std::string toString() {
    return fmt::format(" len_ = {},  BUFFER_SIZE = {}", len_, BUFFER_SIZE);
  }

  FLATTEN bool skipHeader(
      RowVectorLayout& layout,
      int32_t dstPos,
      int32_t offset,
      int32_t& outputLen,
      bool& layoutEnd) {
    auto prevLayout = layout_;
    layout_ = (RowVectorLayout)(*((uint8_t*)compressBufferPtr_ + srcPos_));
    ++srcPos_;
    frameFinished_ = false;
    // frame layout switch
    if (layout_ != prevLayout && prevLayout != RowVectorLayout::kInvalid &&
        dstPos != 0) {
      needRead_ = false;
      outputLen = dstPos - offset;
      layout = prevLayout;
      layoutEnd = true;
      return true;
    }
    return false;
  }

  std::unique_ptr<ZstdStreamCompressor> zstdCompressor_;
  std::unique_ptr<ZstdStreamCompressor> parallelZstdCompressor_;

  std::unique_ptr<ZstdStreamDecompressor> zstdDecompressor_;

  ZSTD_CCtx* currentCCtx_{nullptr};

  std::unique_ptr<arrow::Buffer> uncompressedBuffer_{nullptr};
  uint8_t* uncompressBufferPtr_;
  std::unique_ptr<arrow::Buffer> compressedBuffer_{nullptr};
  uint8_t* compressBufferPtr_;
  std::unique_ptr<arrow::ResizableBuffer> largeBuffer_{nullptr};
  size_t len_{0};
  const size_t BUFFER_SIZE;
  const size_t MAX_COMPRESS_SIZE;
  arrow::MemoryPool* pool_;
  uint64_t compressTime_{0};
  uint64_t writeTime_{0};

  // for Decompress
  size_t srcSize_{0};
  size_t srcPos_{0};
  bool needRead_{true};
  bool frameFinished_{true};
  RowVectorLayout layout_{RowVectorLayout::kInvalid};

  // for checksum
  bool checksumEnabled_{true};
};

} // namespace bytedance::bolt::shuffle::sparksql
