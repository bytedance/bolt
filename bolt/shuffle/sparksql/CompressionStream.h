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
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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
namespace bytedance::bolt::shuffle::sparksql {
#define DEFAULT_STREAM_COMPRESSION_LEVEL 1

class ZstdStreamCodec {
 public:
  // todo: make configurable
  static constexpr int32_t kParallelCompressionThreshold = 2 * 1024 * 1024;
  static constexpr int32_t kWorkerNumber = 2;

  ZstdStreamCodec(
      int32_t compressionLevel,
      bool compress,
      arrow::MemoryPool* pool)
      : compressionLevel_(compressionLevel),
        BUFFER_SIZE(compress ? ZSTD_CStreamInSize() : 0),
        MAX_COMPRESS_SIZE(
            compress ? maxCompressedSize(BUFFER_SIZE) * 2
                     : ZSTD_DStreamInSize()),
        pool_(pool) {
    if (compress) {
      auto status = initCCtx();
      BOLT_CHECK(status.ok(), "initCCtx failed : " + status.message());
      uncompressedBuffer_ =
          arrow::AllocateResizableBuffer(BUFFER_SIZE, pool_).ValueOrDie();
      uncompressBufferPtr_ = uncompressedBuffer_->mutable_data();
      compressedBuffer_ =
          arrow::AllocateResizableBuffer(MAX_COMPRESS_SIZE, pool_).ValueOrDie();
      compressBufferPtr_ = compressedBuffer_->mutable_data();
      LOG(INFO) << "ZstdStreamCodec isCompress = " << compress
                << ", cin buffer = " << BUFFER_SIZE
                << ", cout = " << MAX_COMPRESS_SIZE
                << ", compressionLevel = " << compressionLevel;

    } else {
      zstdDCtx_ = ZSTD_createDCtx();
      compressedBuffer_ =
          arrow::AllocateResizableBuffer(MAX_COMPRESS_SIZE, pool_).ValueOrDie();
      compressBufferPtr_ = compressedBuffer_->mutable_data();
    }
  }

  ~ZstdStreamCodec() {
    if (zstdCCtx_) {
      ZSTD_freeCCtx(zstdCCtx_);
      zstdCCtx_ = nullptr;
    }

    if (zstdCCtx2_) {
      ZSTD_freeCCtx(zstdCCtx2_);
      zstdCCtx2_ = nullptr;
    }

    if (zstdDCtx_) {
      ZSTD_freeDCtx(zstdDCtx_);
      zstdDCtx_ = nullptr;
    }
  }

  arrow::Status resetDCtx() {
    BOLT_DCHECK(zstdDCtx_, "zstdDCtx_ should not be nullptr");
    auto ret = ZSTD_DCtx_reset(zstdDCtx_, ZSTD_reset_session_only);
    if (ZSTD_isError(ret)) {
      return arrow::Status::Invalid("ZSTD_DCtx_reset failed");
    }
    return arrow::Status::OK();
  }

  size_t maxCompressedSize(size_t size) {
    return ZSTD_compressBound(size);
  }

  arrow::Status CompressAndFlush(
      folly::Range<uint8_t**> rows,
      arrow::io::OutputStream* outputStream,
      const int64_t rawSize,
      const RowVectorLayout layout) {
    if (rawSize >= kParallelCompressionThreshold) {
      currentCCtx_ = zstdCCtx2_;
    } else {
      currentCCtx_ = zstdCCtx_;
    }
    RETURN_NOT_OK(outputStream->Write((uint8_t*)(&layout), 1));
    for (auto i = 0; i < rows.size(); ++i) {
      uint8_t* input = rows.data()[i];
      // rowSize + 4 byte header
      auto inputSize = *((int32_t*)input) + sizeof(int32_t);
      if (inputSize >= BUFFER_SIZE || inputSize + len_ > BUFFER_SIZE) {
        // compress cached data first
        if (len_) {
          ZSTD_outBuffer outBuf{compressBufferPtr_, MAX_COMPRESS_SIZE, 0};
          ZSTD_inBuffer inBuf{uncompressBufferPtr_, len_, 0};
          RETURN_NOT_OK(CompressInternal(inBuf, outBuf, outputStream));
          if (outBuf.pos) {
            bytedance::bolt::NanosecondTimer timer(&writeTime_);
            RETURN_NOT_OK(outputStream->Write(compressBufferPtr_, outBuf.pos));
          }
          len_ = 0;
        }
        // input row is extremely large
        if (inputSize >= BUFFER_SIZE) {
          ZSTD_inBuffer inBuf{input, (size_t)inputSize, 0};
          auto outputSize = maxCompressedSize(inputSize) * 2;
          if (largeBuffer_) {
            RETURN_NOT_OK(largeBuffer_->Resize(outputSize));
          } else {
            ARROW_ASSIGN_OR_RAISE(
                largeBuffer_,
                arrow::AllocateResizableBuffer(outputSize, pool_));
          }
          ZSTD_outBuffer outBuf{largeBuffer_->mutable_data(), outputSize, 0};
          RETURN_NOT_OK(CompressInternal(inBuf, outBuf, outputStream));
          if (outBuf.pos) {
            bytedance::bolt::NanosecondTimer timer(&writeTime_);
            RETURN_NOT_OK(
                outputStream->Write(largeBuffer_->data(), outBuf.pos));
          }
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
    RETURN_NOT_OK(endCompressionStream(outputStream));
    RETURN_NOT_OK(resetCCtx());
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

      ZSTD_inBuffer inBuf{compressBufferPtr_, srcSize_, srcPos_};
      ZSTD_outBuffer outBuf{dst, (size_t)dstSize, (size_t)dstPos};
      size_t size = ZSTD_decompressStream(zstdDCtx_, &outBuf, &inBuf);
      srcPos_ = inBuf.pos;
      dstPos = outBuf.pos;

      if (ZSTD_isError(size)) [[unlikely]] {
        return arrow::Status::Invalid(
            "ZSTD_decompressStream error : " +
            std::string(ZSTD_getErrorName(size)) + ", outputSize = " +
            std::to_string(dstSize) + ", offset = " + std::to_string(offset) +
            ", dstPos = " + std::to_string(dstPos) +
            ", inputSize = " + std::to_string(srcSize_) +
            ", inputPos = " + std::to_string(srcPos_));
      }

      // we have completed a frame
      if (size == 0) {
        frameFinished_ = true;
        // need to read from the inputStream only if source buffer are consumed
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
      ZSTD_inBuffer& inBuf,
      ZSTD_outBuffer& outBuf,
      arrow::io::OutputStream* outputStream) {
    uint64_t compressTime = 0, writeTime = 0;
    {
      bytedance::bolt::NanosecondTimer timer(&compressTime);
      do {
        auto ret = ZSTD_compressStream2(
            currentCCtx_, &outBuf, &inBuf, ZSTD_e_continue);
        if (ZSTD_isError(ret)) [[unlikely]] {
          return arrow::Status::Invalid(
              "ZSTD_compressStream2 failed : " +
              std::string(ZSTD_getErrorName(ret)));
        }
        if (ret > 0 && outBuf.pos == outBuf.size) {
          bytedance::bolt::NanosecondTimer timer1(&writeTime);
          RETURN_NOT_OK(outputStream->Write(outBuf.dst, outBuf.pos));
          outBuf.pos = 0;
        }
      } while (inBuf.pos != inBuf.size);
    }
    compressTime_ += (compressTime - writeTime);
    writeTime_ += writeTime;
    return arrow::Status::OK();
  }

  arrow::Status endCompressionStream(arrow::io::OutputStream* outputStream) {
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

  arrow::Status initCCtx() {
    zstdCCtx_ = ZSTD_createCCtx();
    auto ret = ZSTD_CCtx_setParameter(
        zstdCCtx_, ZSTD_c_compressionLevel, compressionLevel_);
    if (ZSTD_isError(ret)) {
      return arrow::Status::Invalid(
          "ZSTD_CCtx_setParameter 1 compressionLevel = " +
          std::to_string(compressionLevel_));
    }
    zstdCCtx2_ = ZSTD_createCCtx();
    ret = ZSTD_CCtx_setParameter(
        zstdCCtx2_, ZSTD_c_compressionLevel, compressionLevel_);
    if (ZSTD_isError(ret)) [[unlikely]] {
      return arrow::Status::Invalid(
          "ZSTD_CCtx_setParameter 2 compressionLevel = " +
          std::to_string(compressionLevel_));
    }

    ret = ZSTD_CCtx_setParameter(zstdCCtx2_, ZSTD_c_nbWorkers, kWorkerNumber);
    if (ZSTD_isError(ret)) [[unlikely]] {
      return arrow::Status::Invalid(
          "ZSTD_CCtx_setParameter 2 ZSTD_c_nbWorkers = " +
          std::to_string(kWorkerNumber));
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
    return fmt::format(
        " len_ = {},  BUFFER_SIZE = {},  compressionLevel_ = {}",
        len_,
        BUFFER_SIZE,
        compressionLevel_);
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

  ZSTD_CCtx* zstdCCtx_{nullptr};
  ZSTD_CCtx* zstdCCtx2_{nullptr};
  ZSTD_CCtx* currentCCtx_{nullptr};

  ZSTD_DCtx* zstdDCtx_{nullptr};
  int32_t compressionLevel_;
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
};

} // namespace bytedance::bolt::shuffle::sparksql
