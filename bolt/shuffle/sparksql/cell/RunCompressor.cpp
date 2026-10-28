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

#include <algorithm>
#include <cstring>

#include "bolt/common/base/Exceptions.h"
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
/// with room past it for the worst case of one piece and the trailer.
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

} // namespace

void BufferRunSink::write(const void* data, size_t bytes) {
  appendBytes(buffer_, data, bytes, pool_);
}

void BufferRunSink::patch(uint64_t position, const void* data, size_t bytes) {
  BOLT_CHECK_LE(position + bytes, this->position(), "run patch beyond its end");
  ::memcpy(buffer_->asMutable<char>() + position, data, bytes);
}

RunCompressor::RunCompressor(
    const PartitionWriterOptions& options,
    bool streaming,
    memory::MemoryPool* pool,
    uint64_t* compressTimeNs)
    : pool_(pool), compressTimeNs_(compressTimeNs) {
  if (options.compressionType == arrow::Compression::UNCOMPRESSED) {
    return;
  }
  const CodecOptions codecOptions{
      getCodecBackend(options.codecBackend),
      options.compressionLevel,
      options.checksumEnabled};
  codec_ = createCodec(options.compressionType, codecOptions);
  if (!streaming) {
    return;
  }
  try {
    // Independent blocks: the stage is reused for every block, so a linked
    // block would compress against a copied-aside dictionary.
    auto streamOptions = codecOptions;
    streamOptions.independentBlocks = true;
    streamCompressor_ = StreamCompressor::create(
        fromArrowCodecType(options.compressionType), streamOptions);
  } catch (const std::exception&) {
    // No stream form for this codec or backend: one-shot it is.
    return;
  }
  stage_.resize(kStreamPieceBytes);
  out_.resize(
      kStreamOutBytes +
      streamCompressor_->recommendedOutputSize(kStreamPieceBytes) +
      kStreamOutSlackBytes);
}

RunCompressor::~RunCompressor() = default;

bool RunCompressor::write(
    RunSink& sink,
    uint32_t numStreams,
    const uint64_t* sizes,
    RunStreamVisitor visit) {
  uint64_t total = 0;
  for (uint32_t s = 0; s < numStreams; ++s) {
    total += sizes[s];
  }
  if (codec_ == nullptr || total < kMinCompressRunBytes) {
    writeStored(sink, numStreams, sizes, visit);
    return true;
  }
  if (total < kMinSeparateRunBytes) {
    std::vector<char> header(runHeaderBytes(numStreams));
    const uint64_t headerAt = sink.position();
    sink.write(header.data(), header.size());
    const auto visitAll = [&](RunPieceFn fn) {
      for (uint32_t s = 0; s < numStreams; ++s) {
        visit(s, fn);
      }
    };
    const auto frame = writeFrame(sink, total, visitAll, /*probe=*/false);
    const uint64_t stored = frame.stored;
    if (stored == 0) {
      visitAll(
          [&](const char* data, uint32_t bytes) { sink.write(data, bytes); });
    }
    writeRunHeader(
        header.data(),
        stored == 0 ? RunLayout::kCombinedUncompressed : RunLayout::kCombined,
        stored == 0 ? total : stored,
        sizes,
        numStreams);
    sink.patch(headerAt, header.data(), header.size());
    return frame.final;
  }
  // SEPARATE: the stored size per stream, then the decoded size - 0 for a
  // stream stored as it is.
  bool final = true;
  std::vector<uint64_t> header(2ull * numStreams, 0);
  const uint64_t headerAt = sink.position();
  const auto layout = static_cast<uint8_t>(RunLayout::kSeparate);
  sink.write(&layout, 1);
  sink.write(header.data(), header.size() * sizeof(uint64_t));
  for (uint32_t s = 0; s < numStreams; ++s) {
    const uint64_t bytes = sizes[s];
    header[s] = bytes;
    if (bytes == 0) {
      continue;
    }
    const auto visitStream = [&](RunPieceFn fn) { visit(s, fn); };
    const auto frame = bytes >= kMinCompressRunBytes
        ? writeFrame(sink, bytes, visitStream, /*probe=*/true)
        : Frame{};
    if (frame.stored > 0) {
      header[s] = frame.stored;
      header[numStreams + s] = bytes;
      continue;
    }
    final &= frame.final;
    visitStream([&](const char* data, uint32_t pieceBytes) {
      sink.write(data, pieceBytes);
    });
  }
  sink.patch(headerAt + 1, header.data(), header.size() * sizeof(uint64_t));
  return final;
}

void RunCompressor::writeStored(
    RunSink& sink,
    uint32_t numStreams,
    const uint64_t* sizes,
    RunStreamVisitor visit) {
  uint64_t total = 0;
  for (uint32_t s = 0; s < numStreams; ++s) {
    total += sizes[s];
  }
  std::vector<char> header(runHeaderBytes(numStreams));
  writeRunHeader(
      header.data(),
      RunLayout::kCombinedUncompressed,
      total,
      sizes,
      numStreams);
  sink.write(header.data(), header.size());
  for (uint32_t s = 0; s < numStreams; ++s) {
    visit(
        s, [&](const char* data, uint32_t bytes) { sink.write(data, bytes); });
  }
}

void RunCompressor::releaseWorkspace() {
  frame_.reset();
  compressed_.reset();
}

RunCompressor::Frame RunCompressor::writeFrame(
    RunSink& sink,
    uint64_t bytes,
    FrameVisitor visit,
    bool probe) {
  if (streamCompressor_ != nullptr) {
    return Frame{streamFrame(sink, bytes, visit, probe)};
  }
  return oneShotFrame(sink, bytes, visit);
}

uint64_t RunCompressor::streamFrame(
    RunSink& sink,
    uint64_t bytes,
    FrameVisitor visit,
    bool probe) {
  auto& compressor = *streamCompressor_;
  compressor.reset();
  const int64_t outCapacity = static_cast<int64_t>(out_.size());
  const int64_t pieceBound =
      outCapacity - kStreamOutBytes; // worst case of one piece, plus slack
  int64_t held = 0; // frame bytes in out_, not yet in the sink
  bool written = false; // whether some of the frame is in the sink
  uint64_t fed = 0;
  uint64_t emitted = 0;
  size_t staged = 0;
  bool storedInstead = false;
  const auto makeRoom = [&]() {
    if (outCapacity - held < pieceBound) {
      sink.write(out_.data(), static_cast<size_t>(held));
      held = 0;
      written = true;
    }
  };
  const auto compressStaged = [&]() {
    if (staged == 0) {
      return;
    }
    makeRoom();
    StreamCompressResult result;
    {
      NanosecondTimer timer(compressTimeNs_);
      result = compressor.compress(
          stage_.data(),
          static_cast<int64_t>(staged),
          out_.data() + held,
          outCapacity - held);
    }
    BOLT_CHECK_EQ(
        result.bytesRead,
        static_cast<int64_t>(staged),
        "stream compressor left input behind");
    held += result.bytesWritten;
    emitted += static_cast<uint64_t>(result.bytesWritten);
    fed += staged;
    staged = 0;
    if (probe && !written && fed >= kStreamProbeBytes &&
        doesNotShrink(emitted, fed)) {
      storedInstead = true;
    }
  };
  visit([&](const char* data, uint32_t bytes) {
    while (bytes > 0 && !storedInstead) {
      const size_t take = std::min<size_t>(bytes, stage_.size() - staged);
      ::memcpy(stage_.data() + staged, data, take);
      staged += take;
      data += take;
      bytes -= static_cast<uint32_t>(take);
      if (staged == stage_.size()) {
        compressStaged();
      }
    }
  });
  if (!storedInstead) {
    compressStaged();
  }
  if (storedInstead) {
    return 0;
  }
  for (;;) {
    makeRoom();
    StreamEndResult end;
    {
      NanosecondTimer timer(compressTimeNs_);
      end = compressor.end(out_.data() + held, outCapacity - held);
    }
    held += end.bytesWritten;
    emitted += static_cast<uint64_t>(end.bytesWritten);
    if (end.noMoreOutput) {
      break;
    }
  }
  if (!written && doesNotShrink(emitted, bytes)) {
    return 0;
  }
  if (held > 0) {
    sink.write(out_.data(), static_cast<size_t>(held));
  }
  return emitted;
}

RunCompressor::Frame
RunCompressor::oneShotFrame(RunSink& sink, uint64_t bytes, FrameVisitor visit) {
  // The workspaces come from the task pool, and a spill or a seal is often
  // the moment that pool is exhausted: the frame is then stored rather
  // than failing the very operation meant to relieve the pressure.
  try {
    resetBuffer(frame_, bytes, pool_);
    resetBuffer(compressed_, codec_->maxCompressedLen(bytes), pool_);
  } catch (const std::exception&) {
    releaseWorkspace();
    return Frame{0, /*final=*/false};
  }
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
    return Frame{};
  }
  sink.write(compressed_->as<char>(), static_cast<size_t>(written));
  return Frame{static_cast<uint64_t>(written)};
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
