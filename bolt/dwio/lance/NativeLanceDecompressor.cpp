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

#include "bolt/dwio/lance/NativeLanceDecompressor.h"

#include <atomic>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <vector>

#include <folly/lang/Bits.h>
#include <lz4.h>
#include <zstd.h>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::lance::reader {
namespace {

template <typename T>
T readLittleEndian(const char* data) {
  return folly::Endian::little(folly::loadUnaligned<T>(data));
}

struct ZstdContextDeleter {
  void operator()(ZSTD_DCtx* context) const {
    ZSTD_freeDCtx(context);
  }
};

ZSTD_DCtx* zstdContext() {
  thread_local auto context =
      std::unique_ptr<ZSTD_DCtx, ZstdContextDeleter>(ZSTD_createDCtx());
  BOLT_CHECK_NOT_NULL(context.get(), "Failed to create Lance zstd context");
  return context.get();
}

} // namespace

NativeLanceCodecAccess NativeLanceDecompressor::accessMode(
    std::string_view scheme) {
  if (scheme == "zstd") {
    return NativeLanceCodecAccess::kSequentialFrame;
  }
  if (scheme == "lz4") {
    return NativeLanceCodecAccess::kWholeBuffer;
  }
  BOLT_UNSUPPORTED("Unsupported Lance compression scheme: {}", scheme);
}

BufferPtr NativeLanceDecompressor::decompress(
    std::string_view scheme,
    const BufferPtr& compressed,
    memory::MemoryPool& pool) {
  const auto* source = compressed->as<char>();
  auto sourceSize = compressed->size();
  uint64_t outputSize = 0;
  if (scheme == "zstd") {
    constexpr uint32_t kZstdMagic = 0xFD2FB528;
    const auto rawFrame = sourceSize < 8 ||
        (readLittleEndian<uint32_t>(source) == kZstdMagic &&
         (static_cast<uint8_t>(source[4]) & 0x10) == 0);
    if (rawFrame) {
      const auto frameSize = ZSTD_getFrameContentSize(source, sourceSize);
      BOLT_CHECK_NE(
          frameSize, ZSTD_CONTENTSIZE_ERROR, "Invalid Lance zstd frame");
      if (frameSize != ZSTD_CONTENTSIZE_UNKNOWN) {
        auto output = AlignedBuffer::allocate<char>(frameSize, &pool);
        const auto decoded = ZSTD_decompressDCtx(
            zstdContext(),
            output->asMutable<char>(),
            frameSize,
            source,
            sourceSize);
        BOLT_CHECK(
            !ZSTD_isError(decoded),
            "Failed to decompress Lance zstd frame: {}",
            ZSTD_getErrorName(decoded));
        BOLT_CHECK_EQ(decoded, frameSize);
        return output;
      }
      auto remaining = ZSTD_initDStream(zstdContext());
      BOLT_CHECK(
          !ZSTD_isError(remaining),
          "Failed to initialize Lance zstd stream: {}",
          ZSTD_getErrorName(remaining));
      auto capacity = std::max<size_t>(ZSTD_DStreamOutSize(), 64 * 1024);
      auto output = AlignedBuffer::allocate<char>(capacity, &pool);
      ZSTD_inBuffer input{source, sourceSize, 0};
      size_t produced = 0;
      while (input.pos < input.size || remaining != 0) {
        if (produced == output->size()) {
          BOLT_CHECK_LE(
              output->size(),
              static_cast<size_t>(std::numeric_limits<int32_t>::max()) / 2,
              "Decoded Lance zstd buffer exceeds 2 GiB");
          AlignedBuffer::reallocate<char>(&output, output->size() * 2);
        }
        const auto inputBefore = input.pos;
        ZSTD_outBuffer out{
            output->asMutable<char>() + produced, output->size() - produced, 0};
        remaining = ZSTD_decompressStream(zstdContext(), &out, &input);
        BOLT_CHECK(
            !ZSTD_isError(remaining),
            "Failed to decompress Lance zstd stream: {}",
            ZSTD_getErrorName(remaining));
        produced += out.pos;
        BOLT_CHECK(
            out.pos > 0 || input.pos > inputBefore || remaining == 0,
            "Truncated Lance zstd stream");
      }
      output->setSize(produced);
      return output;
    }

    BOLT_CHECK_GE(sourceSize, sizeof(uint64_t));
    outputSize = readLittleEndian<uint64_t>(source);
    source += sizeof(uint64_t);
    sourceSize -= sizeof(uint64_t);
    auto output = AlignedBuffer::allocate<char>(outputSize, &pool);
    const auto decoded = ZSTD_decompressDCtx(
        zstdContext(),
        output->asMutable<char>(),
        outputSize,
        source,
        sourceSize);
    BOLT_CHECK(
        !ZSTD_isError(decoded),
        "Failed to decompress Lance zstd buffer: {}",
        ZSTD_getErrorName(decoded));
    BOLT_CHECK_EQ(decoded, outputSize);
    return output;
  }

  if (scheme == "lz4") {
    BOLT_CHECK_GE(sourceSize, sizeof(uint32_t));
    outputSize = readLittleEndian<uint32_t>(source);
    source += sizeof(uint32_t);
    sourceSize -= sizeof(uint32_t);
    BOLT_CHECK_LE(
        outputSize, static_cast<uint64_t>(std::numeric_limits<int>::max()));
    BOLT_CHECK_LE(
        sourceSize, static_cast<size_t>(std::numeric_limits<int>::max()));
    auto output = AlignedBuffer::allocate<char>(outputSize, &pool);
    const auto decoded = LZ4_decompress_safe(
        source,
        output->asMutable<char>(),
        static_cast<int>(sourceSize),
        static_cast<int>(outputSize));
    BOLT_CHECK_GE(decoded, 0, "Failed to decompress Lance lz4 buffer");
    BOLT_CHECK_EQ(static_cast<uint64_t>(decoded), outputSize);
    return output;
  }

  BOLT_UNSUPPORTED("Unsupported Lance compression scheme: {}", scheme);
}

class NativeLanceZstdStream::Impl {
 public:
  Impl(
      uint64_t compressedOffset,
      uint64_t compressedLength,
      memory::MemoryPool& pool,
      Read read,
      uint64_t chunkBytes,
      uint64_t historyBytes)
      : compressedOffset_(compressedOffset),
        compressedLength_(compressedLength),
        pool_(pool),
        read_(std::move(read)),
        chunkBytes_(chunkBytes),
        historyCapacity_(historyBytes),
        context_(ZSTD_createDCtx()) {
    BOLT_CHECK_GT(compressedLength_, 0);
    BOLT_CHECK_GT(chunkBytes_, 0);
    BOLT_CHECK_NOT_NULL(context_.get(), "Failed to create Lance zstd context");
    reset(false);
  }

  BufferPtr decodeRange(uint64_t decodedOffset, uint64_t decodedLength) {
    std::lock_guard<std::mutex> lock(mutex_);
    BOLT_CHECK_LE(
        decodedOffset, std::numeric_limits<uint64_t>::max() - decodedLength);
    auto output = AlignedBuffer::allocate<char>(decodedLength, &pool_);
    if (decodedLength == 0) {
      return output;
    }

    uint64_t outputOffset = 0;
    if (decodedOffset < decodedOffset_) {
      const auto requestedEnd = decodedOffset + decodedLength;
      if (decodedOffset >= historyStart_ && requestedEnd > decodedOffset_) {
        const auto historyBytes = decodedOffset_ - decodedOffset;
        const auto copyBytes = std::min(historyBytes, decodedLength);
        std::memcpy(
            output->asMutable<char>(),
            history_.data() + (decodedOffset - historyStart_),
            copyBytes);
        decodedOffset += copyBytes;
        outputOffset += copyBytes;
      } else if (
          decodedOffset >= historyStart_ && requestedEnd <= decodedOffset_) {
        std::memcpy(
            output->asMutable<char>(),
            history_.data() + (decodedOffset - historyStart_),
            decodedLength);
        return output;
      } else {
        reset(true);
      }
    }

    if (decodedOffset > decodedOffset_) {
      skip(decodedOffset - decodedOffset_);
    }
    BOLT_CHECK_EQ(decodedOffset, decodedOffset_);
    decodeExact(
        output->asMutable<char>() + outputOffset, decodedLength - outputOffset);
    return output;
  }

  uint64_t decodedOffset() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return decodedOffset_;
  }

  uint64_t restartCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return restartCount_;
  }

  uint64_t retainedBytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return (input_ == nullptr ? 0 : input_->size()) + history_.capacity();
  }

  bool finished() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return frameFinished_ ||
        (expectedDecodedBytes_.has_value() &&
         decodedOffset_ == *expectedDecodedBytes_);
  }

  std::optional<std::pair<uint64_t, uint64_t>> nextCompressedRange() const {
    const auto length = nextCompressedLength_.load(std::memory_order_acquire);
    if (length == 0) {
      return std::nullopt;
    }
    return std::pair<uint64_t, uint64_t>{
        nextCompressedOffset_.load(std::memory_order_relaxed), length};
  }

 private:
  void reset(bool restart) {
    if (restart) {
      ++restartCount_;
    }
    const auto resetResult = ZSTD_initDStream(context_.get());
    BOLT_CHECK(
        !ZSTD_isError(resetResult),
        "Failed to reset Lance zstd stream: {}",
        ZSTD_getErrorName(resetResult));

    const auto firstChunkBytes = std::min(chunkBytes_, compressedLength_);
    input_ = read_(compressedOffset_, firstChunkBytes);
    BOLT_CHECK_NOT_NULL(input_);
    BOLT_CHECK_EQ(input_->size(), firstChunkBytes);
    const auto prefixBytes = std::min<uint64_t>(firstChunkBytes, 8);
    constexpr uint32_t kZstdMagic = 0xFD2FB528;
    const auto rawFrame = prefixBytes < 8 ||
        (readLittleEndian<uint32_t>(input_->as<char>()) == kZstdMagic &&
         (static_cast<uint8_t>(input_->as<char>()[4]) & 0x10) == 0);
    BOLT_CHECK(rawFrame || prefixBytes == sizeof(uint64_t));
    expectedDecodedBytes_ = rawFrame
        ? std::nullopt
        : std::make_optional(readLittleEndian<uint64_t>(input_->as<char>()));
    compressedPosition_ = firstChunkBytes;
    inputPosition_ = rawFrame ? 0 : sizeof(uint64_t);
    updateNextCompressedRange();
    decodedOffset_ = 0;
    historyStart_ = 0;
    history_.clear();
    frameFinished_ = false;
  }

  void ensureInput() {
    if (input_ != nullptr && inputPosition_ < input_->size()) {
      return;
    }
    BOLT_CHECK_LT(
        compressedPosition_, compressedLength_, "Truncated Lance zstd stream");
    const auto bytes =
        std::min(chunkBytes_, compressedLength_ - compressedPosition_);
    input_ = read_(compressedOffset_ + compressedPosition_, bytes);
    BOLT_CHECK_NOT_NULL(input_);
    BOLT_CHECK_EQ(input_->size(), bytes);
    compressedPosition_ += bytes;
    inputPosition_ = 0;
    updateNextCompressedRange();
  }

  void updateNextCompressedRange() {
    if (compressedPosition_ >= compressedLength_) {
      nextCompressedLength_.store(0, std::memory_order_release);
      return;
    }
    nextCompressedOffset_.store(
        compressedOffset_ + compressedPosition_, std::memory_order_relaxed);
    nextCompressedLength_.store(
        std::min(chunkBytes_, compressedLength_ - compressedPosition_),
        std::memory_order_release);
  }

  void appendHistory(const char* bytes, uint64_t size, uint64_t decodedBegin) {
    if (historyCapacity_ == 0 || size == 0) {
      return;
    }
    if (size >= historyCapacity_) {
      history_.assign(bytes + size - historyCapacity_, bytes + size);
      historyStart_ = decodedBegin + size - historyCapacity_;
      return;
    }
    if (history_.empty()) {
      historyStart_ = decodedBegin;
    }
    const auto overflow = history_.size() + size > historyCapacity_
        ? history_.size() + size - historyCapacity_
        : 0;
    if (overflow > 0) {
      history_.erase(history_.begin(), history_.begin() + overflow);
      historyStart_ += overflow;
    }
    history_.insert(history_.end(), bytes, bytes + size);
  }

  void decodeExact(char* output, uint64_t size) {
    uint64_t produced = 0;
    while (produced < size) {
      BOLT_CHECK(!frameFinished_, "Lance zstd range exceeds decoded frame");
      ensureInput();
      ZSTD_inBuffer input{input_->as<char>(), input_->size(), inputPosition_};
      ZSTD_outBuffer decoded{output + produced, size - produced, 0};
      const auto inputBefore = input.pos;
      const auto remaining =
          ZSTD_decompressStream(context_.get(), &decoded, &input);
      BOLT_CHECK(
          !ZSTD_isError(remaining),
          "Failed to decompress Lance zstd stream: {}",
          ZSTD_getErrorName(remaining));
      inputPosition_ = input.pos;
      appendHistory(output + produced, decoded.pos, decodedOffset_);
      produced += decoded.pos;
      decodedOffset_ += decoded.pos;
      BOLT_CHECK(
          !expectedDecodedBytes_.has_value() ||
          decodedOffset_ <= *expectedDecodedBytes_);
      frameFinished_ = remaining == 0;
      if (frameFinished_ ||
          (expectedDecodedBytes_.has_value() &&
           decodedOffset_ == *expectedDecodedBytes_)) {
        nextCompressedLength_.store(0, std::memory_order_release);
      }
      BOLT_CHECK(
          decoded.pos > 0 || input.pos > inputBefore,
          "Truncated Lance zstd stream");
    }
  }

  void skip(uint64_t size) {
    std::vector<char> scratch(
        std::min<uint64_t>(chunkBytes_, std::max<uint64_t>(size, 1)));
    uint64_t skipped = 0;
    while (skipped < size) {
      const auto bytes = std::min<uint64_t>(scratch.size(), size - skipped);
      decodeExact(scratch.data(), bytes);
      skipped += bytes;
    }
  }

  const uint64_t compressedOffset_;
  const uint64_t compressedLength_;
  memory::MemoryPool& pool_;
  const Read read_;
  const uint64_t chunkBytes_;
  const uint64_t historyCapacity_;
  std::unique_ptr<ZSTD_DCtx, ZstdContextDeleter> context_;
  BufferPtr input_;
  uint64_t compressedPosition_{0};
  size_t inputPosition_{0};
  uint64_t decodedOffset_{0};
  uint64_t historyStart_{0};
  std::vector<char> history_;
  uint64_t restartCount_{0};
  bool frameFinished_{false};
  std::optional<uint64_t> expectedDecodedBytes_;
  std::atomic<uint64_t> nextCompressedOffset_{0};
  std::atomic<uint64_t> nextCompressedLength_{0};
  mutable std::mutex mutex_;
};

NativeLanceZstdStream::NativeLanceZstdStream(
    uint64_t compressedOffset,
    uint64_t compressedLength,
    memory::MemoryPool& pool,
    Read read,
    uint64_t chunkBytes,
    uint64_t historyBytes)
    : impl_(std::make_unique<Impl>(
          compressedOffset,
          compressedLength,
          pool,
          std::move(read),
          chunkBytes,
          historyBytes)) {}

NativeLanceZstdStream::~NativeLanceZstdStream() = default;

BufferPtr NativeLanceZstdStream::decodeRange(
    uint64_t decodedOffset,
    uint64_t decodedLength) {
  return impl_->decodeRange(decodedOffset, decodedLength);
}

uint64_t NativeLanceZstdStream::decodedOffset() const {
  return impl_->decodedOffset();
}

uint64_t NativeLanceZstdStream::restartCount() const {
  return impl_->restartCount();
}

uint64_t NativeLanceZstdStream::retainedBytes() const {
  return impl_->retainedBytes();
}

bool NativeLanceZstdStream::finished() const {
  return impl_->finished();
}

std::optional<std::pair<uint64_t, uint64_t>>
NativeLanceZstdStream::nextCompressedRange() const {
  return impl_->nextCompressedRange();
}

} // namespace bytedance::bolt::lance::reader
