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

#include "bolt/shuffle/sparksql/cell/RunCompressor.h"

#include <fmt/format.h>
#include <algorithm>
#include <cstring>
#include <limits>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/MemoryCopy.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/time/Timer.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"

namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

/// Input fed to the stream compressor at a time - its independent block
/// size: small pieces are staged up to it, so every call carries a full
/// block, compressed straight from the stage.
constexpr int64_t kStreamPieceBytes = 256 << 10;
/// The output buffer gathers a frame into sink writes of about this size,
/// with room past it for the codec's bound on one piece of fresh input and
/// the trailer. A call may still emit more - deflate also flushes a block
/// carried over from earlier calls - which StreamFrameEncoder copes with.
constexpr int64_t kStreamOutBytes = 512 << 10;
constexpr int64_t kStreamOutSlackBytes = 64;
/// Input after which a frame that has not shrunk the data is stored
/// instead: its first blocks show the stream does not compress.
constexpr uint64_t kStreamProbeBytes = 256 << 10;
// The probe is decided before the output buffer first reaches the sink, so
// a frame stored instead never has bytes to take back.
static_assert(kStreamProbeBytes <= kStreamOutBytes);

/// Whether `stored` bytes of frame are no gain over `bytes` of input: a
/// ratio above 31/32 is not worth a decompression on every read.
bool doesNotShrink(uint64_t stored, uint64_t bytes) {
  return stored + stored / 32 >= bytes;
}

void writeCombinedPrefix(char* out, RunLayout layout, uint64_t stored) {
  out[0] = static_cast<char>(layout);
  memory::storeUnaligned<uint64_t>(out + 1, stored);
}

/// Encodes one frame through a streaming compressor into `sink` using fixed
/// staging and output buffers, optionally bailing out early if the probe block
/// fails to shrink before any output has been flushed to `sink`.
class StreamFrameEncoder {
 public:
  StreamFrameEncoder(
      StreamCompressor& compressor,
      RunSink& sink,
      std::vector<uint8_t>& inputStage,
      std::vector<uint8_t>& outputBuffer,
      uint64_t* compressTimeNs,
      bool probeEarly)
      : compressor_(compressor),
        sink_(sink),
        inputStage_(inputStage),
        outputBuffer_(outputBuffer),
        compressTimeNs_(compressTimeNs),
        probeEarly_(probeEarly),
        outputCapacity_(static_cast<int64_t>(outputBuffer.size())),
        minOutputRoom_(outputCapacity_ - kStreamOutBytes) {
    compressor_.reset();
  }

  void feedInput(const char* data, uint32_t bytes) {
    while (bytes > 0 && !abortedEarly_) {
      const size_t copyBytes =
          std::min<size_t>(bytes, inputStage_.size() - stagedInputBytes_);
      ::memcpy(inputStage_.data() + stagedInputBytes_, data, copyBytes);
      stagedInputBytes_ += copyBytes;
      data += copyBytes;
      bytes -= static_cast<uint32_t>(copyBytes);
      if (stagedInputBytes_ == inputStage_.size()) {
        compressStagedBlock();
      }
    }
  }

  uint64_t finish(uint64_t totalInputBytes) {
    if (!abortedEarly_) {
      compressStagedBlock();
    }
    if (abortedEarly_) {
      return 0;
    }
    finishCompression();
    if (!flushedToSink_ && doesNotShrink(totalOutputBytes_, totalInputBytes)) {
      return 0;
    }
    if (pendingOutputBytes_ > 0) {
      flushOutputToSink();
    }
    return totalOutputBytes_;
  }

  bool abortedEarly() const {
    return abortedEarly_;
  }

 private:
  void flushOutputToSink() {
    sink_.write(outputBuffer_.data(), static_cast<size_t>(pendingOutputBytes_));
    pendingOutputBytes_ = 0;
    flushedToSink_ = true;
  }

  void ensureOutputRoom() {
    if (outputCapacity_ - pendingOutputBytes_ < minOutputRoom_) {
      flushOutputToSink();
    }
  }

  void compressStagedBlock() {
    if (stagedInputBytes_ == 0) {
      return;
    }
    ensureOutputRoom();
    // One call normally consumes the whole stage: minOutputRoom_ covers one
    // block's bound. A codec may still stop short (e.g. deflate flushing a
    // carried-over block), so flush buffered output and feed the remainder.
    size_t consumed = 0;
    while (consumed < stagedInputBytes_) {
      StreamCompressResult result;
      {
        NanosecondTimer timer(compressTimeNs_);
        result = compressor_.compress(
            inputStage_.data() + consumed,
            static_cast<int64_t>(stagedInputBytes_ - consumed),
            outputBuffer_.data() + pendingOutputBytes_,
            outputCapacity_ - pendingOutputBytes_);
      }
      BOLT_CHECK(
          result.bytesRead > 0 || result.bytesWritten > 0 ||
              pendingOutputBytes_ > 0,
          "stream compressor made no progress");
      pendingOutputBytes_ += result.bytesWritten;
      totalOutputBytes_ += static_cast<uint64_t>(result.bytesWritten);
      consumed += static_cast<size_t>(result.bytesRead);
      if (consumed < stagedInputBytes_ && pendingOutputBytes_ > 0) {
        flushOutputToSink();
      }
    }
    inputBytesFed_ += stagedInputBytes_;
    stagedInputBytes_ = 0;
    if (probeEarly_ && !flushedToSink_ && inputBytesFed_ >= kStreamProbeBytes &&
        doesNotShrink(totalOutputBytes_, inputBytesFed_)) {
      abortedEarly_ = true;
    }
  }

  void finishCompression() {
    for (;;) {
      ensureOutputRoom();
      StreamEndResult end;
      {
        NanosecondTimer timer(compressTimeNs_);
        end = compressor_.end(
            outputBuffer_.data() + pendingOutputBytes_,
            outputCapacity_ - pendingOutputBytes_);
      }
      pendingOutputBytes_ += end.bytesWritten;
      totalOutputBytes_ += static_cast<uint64_t>(end.bytesWritten);
      if (end.noMoreOutput) {
        break;
      }
      BOLT_CHECK(
          end.bytesWritten > 0 || pendingOutputBytes_ > 0,
          "stream compressor end() made no progress");
      if (pendingOutputBytes_ > 0) {
        flushOutputToSink();
      }
    }
  }

  StreamCompressor& compressor_;
  RunSink& sink_;
  std::vector<uint8_t>& inputStage_;
  std::vector<uint8_t>& outputBuffer_;
  uint64_t* const compressTimeNs_;
  const bool probeEarly_;
  const int64_t outputCapacity_;
  const int64_t minOutputRoom_;
  size_t stagedInputBytes_{0};
  int64_t pendingOutputBytes_{0};
  uint64_t inputBytesFed_{0};
  uint64_t totalOutputBytes_{0};
  bool flushedToSink_{false};
  bool abortedEarly_{false};
};

} // namespace

uint64_t BufferRunSink::reserve(size_t bytes) {
  const uint64_t start = buffer_ == nullptr ? 0 : buffer_->size();
  if (bytes > 0) {
    resizeBuffer(buffer_, start + bytes, pool_);
    ::memset(buffer_->asMutable<char>() + start, 0, bytes);
  }
  return start;
}

void BufferRunSink::write(const void* data, size_t bytes) {
  appendBytes(buffer_, data, bytes, pool_);
}

void BufferRunSink::patch(uint64_t position, const void* data, size_t bytes) {
  const uint64_t end = buffer_ == nullptr ? 0 : buffer_->size();
  BOLT_CHECK(
      position <= end && bytes <= end - position, "run patch beyond its end");
  if (bytes == 0) {
    return;
  }
  ::memcpy(buffer_->asMutable<char>() + position, data, bytes);
}

RunCompressor::RunCompressor(
    const PartitionWriterOptions& options,
    const CellShuffleOptions& cellOptions,
    uint64_t* compressTimeNs,
    std::unique_ptr<StreamCompressor> streamCompressor)
    : spillPool_(memory::spillMemoryPool()),
      compressTimeNs_(compressTimeNs),
      skipper_(cellOptions.adaptiveCompression) {
  if (options.compressionType == arrow::Compression::UNCOMPRESSED) {
    BOLT_CHECK(
        streamCompressor == nullptr,
        "a stream compressor requires a compressed codec");
    return;
  }
  const CodecOptions codecOptions{
      getCodecBackend(options.codecBackend),
      options.compressionLevel,
      options.checksumEnabled};
  codec_ = createCodec(options.compressionType, codecOptions);
  if (streamCompressor == nullptr && cellOptions.streamCompression) {
    const auto codecType = fromArrowCodecType(options.compressionType);
    if (StreamCompressor::supports(codecType)) {
      // Independent blocks: the stage is reused for every block, so a linked
      // block would compress against a copied-aside dictionary.
      auto streamOptions = codecOptions;
      streamOptions.useIndependentBlocks = true;
      streamOptions.blockSize = kStreamPieceBytes;
      streamCompressor = StreamCompressor::create(codecType, streamOptions);
    }
  }
  if (streamCompressor != nullptr) {
    streamCompressor_ = std::move(streamCompressor);
    streamInputStage_.resize(kStreamPieceBytes);
    streamOutputBuffer_.resize(
        kStreamOutBytes +
        streamCompressor_->recommendedOutputSize(kStreamPieceBytes) +
        kStreamOutSlackBytes);
  }
}

RunCompressor::~RunCompressor() = default;

uint64_t RunCompressor::write(
    RunSink& sink,
    uint32_t numStreams,
    RunStreamSize size,
    RunStreamVisitor visit) {
  sizes_.resize(numStreams);
  uint64_t total = 0;
  for (uint32_t s = 0; s < numStreams; ++s) {
    sizes_[s] = size(s);
    total += sizes_[s];
  }
  if (total == 0) {
    return 0;
  }
  const std::span<const uint64_t> sizes = sizes_;
  if (codec_ == nullptr || total < kMinCompressBytes) {
    storeUncompressed(sink, sizes, total, visit);
    ++stats_.storedRuns;
    if (codec_ != nullptr) {
      ++stats_.tinyFrames;
      stats_.tinyBytes += total;
    }
    return total;
  }
  skipper_.prepareStreams(numStreams);
  if (total < kMinSeparateRunBytes) {
    const uint64_t headerAt = sink.reserve(kCombinedRunDecodedSizesOffset);
    sink.write(sizes.data(), sizes.size_bytes());
    const auto visitAll = [&](RunPieceFn fn) {
      for (uint32_t s = 0; s < numStreams; ++s) {
        visit(s, fn);
      }
    };
    ++stats_.combinedRuns;
    uint64_t stored = 0;
    const bool skipped = skipper_.shouldSkipCombined();
    if (!skipped) {
      stored = writeFrame(sink, total, visitAll, /*probeEarly=*/false);
      skipper_.recordCombined(stored > 0);
    }
    countFrame(stored, total, skipped);
    if (stored == 0) {
      visitAll(
          [&](const char* data, uint32_t bytes) { sink.write(data, bytes); });
    }
    char prefix[kCombinedRunDecodedSizesOffset];
    writeCombinedPrefix(
        prefix,
        stored == 0 ? RunLayout::kCombinedUncompressed : RunLayout::kCombined,
        stored == 0 ? total : stored);
    sink.patch(headerAt, prefix, sizeof(prefix));
    return total;
  }
  // kSeparate: the stored size per stream, then the decoded size - 0 for a
  // stream stored as it is.
  ++stats_.separateRuns;
  const auto layout = static_cast<uint8_t>(RunLayout::kSeparate);
  sink.write(&layout, 1);
  const uint64_t headerAt = sink.reserve(2ull * numStreams * sizeof(uint64_t));
  std::vector<uint64_t> header(2ull * numStreams, 0);
  for (uint32_t s = 0; s < numStreams; ++s) {
    const uint64_t bytes = sizes[s];
    header[s] = bytes;
    if (bytes == 0) {
      continue;
    }
    const auto visitStream = [&](RunPieceFn fn) { visit(s, fn); };
    uint64_t stored = 0;
    if (bytes < kMinCompressBytes) {
      ++stats_.tinyFrames;
      stats_.tinyBytes += bytes;
    } else if (skipper_.shouldSkipStream(s)) {
      countFrame(0, bytes, /*skipped=*/true);
    } else {
      stored = writeFrame(sink, bytes, visitStream, /*probeEarly=*/true);
      skipper_.recordStream(s, stored > 0);
      countFrame(stored, bytes, /*skipped=*/false);
    }
    if (stored > 0) {
      header[s] = stored;
      header[numStreams + s] = bytes;
      continue;
    }
    visitStream([&](const char* data, uint32_t pieceBytes) {
      sink.write(data, pieceBytes);
    });
  }
  sink.patch(headerAt, header.data(), header.size() * sizeof(uint64_t));
  return total;
}

void RunCompressor::storeUncompressed(
    RunSink& sink,
    std::span<const uint64_t> sizes,
    uint64_t total,
    RunStreamVisitor visit) {
  char prefix[kCombinedRunDecodedSizesOffset];
  writeCombinedPrefix(prefix, RunLayout::kCombinedUncompressed, total);
  sink.write(prefix, sizeof(prefix));
  sink.write(sizes.data(), sizes.size_bytes());
  for (uint32_t s = 0; s < sizes.size(); ++s) {
    visit(
        s, [&](const char* data, uint32_t bytes) { sink.write(data, bytes); });
  }
}

void RunCompressor::countFrame(uint64_t stored, uint64_t bytes, bool skipped) {
  if (skipped) {
    ++stats_.skippedFrames;
    stats_.skippedBytes += bytes;
  } else if (stored > 0) {
    ++stats_.compressedFrames;
    stats_.compressedInBytes += bytes;
    stats_.compressedOutBytes += stored;
  } else {
    ++stats_.unshrunkFrames;
    stats_.unshrunkBytes += bytes;
  }
}

std::string RunCompressor::Stats::toString() const {
  const auto mb = [](uint64_t bytes) { return bytes / double(1 << 20); };
  return fmt::format(
      "runs stored={} combined={} separate={}; "
      "frames compressed={} ({:.1f}MB -> {:.1f}MB) unshrunk={} ({:.1f}MB, "
      "early={}) skipped={} ({:.1f}MB) tiny={} ({:.1f}MB); "
      "null bodies compressed={} ({:.1f}MB -> {:.1f}MB) "
      "stored={} ({:.1f}MB)",
      storedRuns,
      combinedRuns,
      separateRuns,
      compressedFrames,
      mb(compressedInBytes),
      mb(compressedOutBytes),
      unshrunkFrames,
      mb(unshrunkBytes),
      earlyStoredFrames,
      skippedFrames,
      mb(skippedBytes),
      tinyFrames,
      mb(tinyBytes),
      compressedNullBodies,
      mb(nullInBytes),
      mb(nullOutBytes),
      storedNullBodies,
      mb(storedNullBytes));
}

uint32_t RunCompressor::compressNullBody(BufferPtr& out, size_t at) {
  if (out == nullptr) {
    BOLT_CHECK_EQ(at, 0, "null body offset past empty buffer");
    return 0;
  }
  BOLT_CHECK_LE(at, out->size(), "null body offset past buffer end");
  const uint64_t bytes = out->size() - at;
  if (codec_ == nullptr || bytes == 0) {
    return 0;
  }
  BOLT_CHECK_LE(
      bytes,
      std::numeric_limits<uint32_t>::max(),
      "null body exceeds u32 size limit");
  const auto stored = [&] {
    ++stats_.storedNullBodies;
    stats_.storedNullBytes += bytes;
    return 0;
  };
  if (bytes < kMinCompressBytes || skipper_.shouldSkipNullBody()) {
    return stored();
  }
  resetBuffer(compressed_, codec_->maxCompressedLen(bytes), spillPool_);
  int64_t written;
  {
    NanosecondTimer timer(compressTimeNs_);
    written = codec_->compress(
        out->as<uint8_t>() + at,
        static_cast<int64_t>(bytes),
        compressed_->asMutable<uint8_t>(),
        static_cast<int64_t>(compressed_->size()));
  }
  BOLT_CHECK_GT(written, 0, "null body compression failed");
  // Kept only at half its size or less: a null body compressing worse holds
  // nulls spread at random, whose few saved bytes cost more codec time, on
  // both sides, than they are worth.
  const bool shrank = static_cast<uint64_t>(written) * 2 <= bytes;
  skipper_.recordNullBody(shrank);
  if (!shrank) {
    return stored();
  }
  ::memcpy(out->asMutable<char>() + at, compressed_->as<char>(), written);
  out->setSize(at + written);
  ++stats_.compressedNullBodies;
  stats_.nullInBytes += bytes;
  stats_.nullOutBytes += static_cast<uint64_t>(written);
  return static_cast<uint32_t>(bytes);
}

void RunCompressor::releaseBuffers() {
  frame_.reset();
  compressed_.reset();
}

uint64_t RunCompressor::writeFrame(
    RunSink& sink,
    uint64_t bytes,
    FrameVisitor visit,
    bool probeEarly) {
  if (streamCompressor_ != nullptr) {
    return streamFrame(sink, bytes, visit, probeEarly);
  }
  return oneShotFrame(sink, bytes, visit);
}

uint64_t RunCompressor::streamFrame(
    RunSink& sink,
    uint64_t totalInputBytes,
    FrameVisitor visit,
    bool probeEarly) {
  StreamFrameEncoder encoder(
      *streamCompressor_,
      sink,
      streamInputStage_,
      streamOutputBuffer_,
      compressTimeNs_,
      probeEarly);
  visit([&](const char* data, uint32_t bytes) {
    encoder.feedInput(data, bytes);
  });
  const uint64_t compressedBytes = encoder.finish(totalInputBytes);
  if (encoder.abortedEarly()) {
    ++stats_.earlyStoredFrames;
  }
  return compressedBytes;
}

uint64_t
RunCompressor::oneShotFrame(RunSink& sink, uint64_t bytes, FrameVisitor visit) {
  resetBuffer(frame_, bytes, spillPool_);
  resetBuffer(compressed_, codec_->maxCompressedLen(bytes), spillPool_);
  char* frame = frame_->asMutable<char>();
  uint64_t offset = 0;
  visit([&](const char* data, uint32_t pieceBytes) {
    ::memcpy(frame + offset, data, pieceBytes);
    offset += pieceBytes;
  });
  BOLT_CHECK_EQ(offset, bytes, "run frame size mismatch");
  int64_t written;
  {
    NanosecondTimer timer(compressTimeNs_);
    written = codec_->compress(
        frame_->as<uint8_t>(),
        static_cast<int64_t>(bytes),
        compressed_->asMutable<uint8_t>(),
        static_cast<int64_t>(compressed_->size()));
  }
  BOLT_CHECK_GT(written, 0, "run frame compression failed");
  if (doesNotShrink(static_cast<uint64_t>(written), bytes)) {
    return 0;
  }
  sink.write(compressed_->as<char>(), static_cast<size_t>(written));
  return static_cast<uint64_t>(written);
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
