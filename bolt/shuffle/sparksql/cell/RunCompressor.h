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

#pragma once

#include <folly/Function.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bolt/buffer/Buffer.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/compression/StreamCodec.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Where a run is written: a spill file, the data file or a payload in
/// memory. A run's header precedes its frames, whose sizes are known only
/// once they are written, so patch() fills them in. A sink may defer a
/// patch, as long as the bytes are in place before anyone reads them.
class RunSink {
 public:
  virtual ~RunSink() = default;

  /// Bytes written so far.
  virtual uint64_t position() const = 0;

  virtual void write(const void* data, size_t bytes) = 0;

  /// Overwrites `bytes` already written at `position`.
  virtual void patch(uint64_t position, const void* data, size_t bytes) = 0;
};

/// A RunSink appending to a buffer from `pool`: a payload sent whole.
class BufferRunSink final : public RunSink {
 public:
  BufferRunSink(BufferPtr& buffer, memory::MemoryPool* pool)
      : buffer_(buffer), pool_(pool) {}

  uint64_t position() const override {
    return buffer_ == nullptr ? 0 : buffer_->size();
  }

  void write(const void* data, size_t bytes) override;

  void patch(uint64_t position, const void* data, size_t bytes) override;

 private:
  BufferPtr& buffer_;
  memory::MemoryPool* const pool_;
};

/// Takes one piece of a stream.
using RunPieceFn = folly::FunctionRef<void(const char* data, uint32_t bytes)>;

/// visit(s, fn) hands over the bytes of stream s, in order, piece by piece.
using RunStreamVisitor =
    folly::FunctionRef<void(uint32_t stream, RunPieceFn fn)>;

/// size(s) is the number of bytes visit(s, fn) hands over.
using RunStreamSize = folly::FunctionRef<uint64_t(uint32_t stream)>;

/// The Cell writer's run compression, one policy for every run it writes:
/// spilled, merged, or sent in an RSS payload. A payload's null body goes
/// through it too, compressed whole as one frame.
///
/// A run below kMinSeparateRunBytes is one COMBINED frame over all its
/// streams; a larger one is SEPARATE, every stream its own frame, so each
/// stream's data alone sets its ratio. A frame below kMinCompressBytes
/// is stored, and so is a frame that does not shrink: it is then cheaper to
/// read as it is. That is decided only while the whole frame so far is
/// still in memory - after its first blocks of input, or at its end when
/// it is no larger - so nothing written is ever taken back: once a frame
/// reaches the sink it is kept compressed. A SEPARATE frame is one stream,
/// whose first blocks speak for the rest; a COMBINED frame, spanning
/// streams, is judged on its whole size only.
///
/// Adaptive, a frame that does not shrink has the next frames of the same
/// kind stored without a codec call, the tenth tried again: frames of a
/// stream for SEPARATE runs, and all COMBINED frames together, which span
/// the same streams. Stream s of every run must therefore be the same
/// stream.
///
/// Streaming compresses a frame straight from the visitor through fixed
/// buffers: no workspace sized by the data, ever. One-shot gathers each
/// frame into a workspace from the spill pool and compresses it at once:
/// spills and seals come when the task's pool is short, and must not fail
/// for the memory meant to relieve it. Codecs with no stream form are
/// always one-shot. Both write the same layouts.
class RunCompressor {
 public:
  /// Compresses with `options`' codec; stores everything when it is
  /// UNCOMPRESSED. `cellOptions` gives the mode (streamCompression,
  /// adaptiveCompression). Codec time is added to `*compressTimeNs`.
  RunCompressor(
      const PartitionWriterOptions& options,
      const CellShuffleOptions& cellOptions,
      uint64_t* compressTimeNs);

  ~RunCompressor();

  /// The codec, to read compressed runs back; null when runs are not
  /// compressed.
  Codec* codec() const {
    return codec_.get();
  }

  /// Writes one run of `numStreams` streams, `size(s)` bytes each, that
  /// `visit` hands over - twice for a frame stored after all. The sizes
  /// come first: they choose the layout and fill the header. A run without
  /// bytes writes nothing.
  void write(
      RunSink& sink,
      uint32_t numStreams,
      RunStreamSize size,
      RunStreamVisitor visit);

  /// Compresses the null body `out` holds from byte `at` to its end, one
  /// frame in place, when it is at least kMinCompressBytes and compresses
  /// to half its size or less; a null body that does not has the next ones
  /// stored without a try, adaptive, as frames of a kind do. Returns its
  /// decoded size, the payload header's nullDecodedSize: 0 when it stays
  /// as it is.
  uint32_t compressNullBody(BufferPtr& out, size_t at);

  /// Gives the one-shot workspaces back to the pool.
  void releaseWorkspace();

  /// Streams frames through `compressor` from now on, in place of the
  /// codec's own stream form; it must write frames the codec reads. For
  /// tests of codec behaviour the real codecs rarely show.
  void testingSetStreamCompressor(std::unique_ptr<StreamCompressor> compressor);

  /// What the compressor did, counted per run and per frame - never per
  /// byte - so the counting costs nothing measurable.
  struct Stats {
    /// Runs by layout. `storedRuns` were under kMinCompressBytes or met
    /// no codec.
    uint64_t storedRuns{0};
    uint64_t combinedRuns{0};
    uint64_t separateRuns{0};
    /// Frames by outcome, with the bytes they took in. A compressed frame
    /// also counts the bytes it wrote; the others are stored as they are.
    uint64_t compressedFrames{0};
    uint64_t compressedInBytes{0};
    uint64_t compressedOutBytes{0};
    /// Tried, and stored because they did not shrink; `earlyFrames` of
    /// them given up after their first blocks.
    uint64_t unshrunkFrames{0};
    uint64_t unshrunkBytes{0};
    uint64_t earlyFrames{0};
    /// Stored without a try: adaptive skips and frames below
    /// kMinCompressBytes.
    uint64_t skippedFrames{0};
    uint64_t skippedBytes{0};
    uint64_t tinyFrames{0};
    uint64_t tinyBytes{0};
    /// Null bodies compressed, with the bytes they took in and wrote, and
    /// stored as they are: too small, skipped or unshrunk.
    uint64_t compressedNullBodies{0};
    uint64_t nullInBytes{0};
    uint64_t nullOutBytes{0};
    uint64_t storedNullBodies{0};
    uint64_t storedNullBytes{0};

    /// One key=value line.
    std::string toString() const;
  };

  /// The same, for one stream's SEPARATE frames.
  struct StreamStats {
    uint64_t tried{0};
    uint64_t compressed{0};
    uint64_t skipped{0};
    uint64_t inBytes{0};
    uint64_t outBytes{0};
  };

  const Stats& stats() const {
    return stats_;
  }

  /// Per stream index, when SEPARATE runs were written.
  const std::vector<StreamStats>& streamStats() const {
    return streamStats_;
  }

 private:
  using FrameVisitor = folly::FunctionRef<void(RunPieceFn fn)>;

  /// Takes `compressor` as the stream form and sizes its buffers.
  void useStreamCompressor(std::unique_ptr<StreamCompressor> compressor);

  /// Writes the `bytes` `visit` hands over as one compressed frame, unless
  /// it does not shrink - judged early from its first blocks when `probe`,
  /// else on its whole size. Returns the compressed frame's size; 0 when it
  /// was not compressed, and nothing was written.
  uint64_t
  writeFrame(RunSink& sink, uint64_t bytes, FrameVisitor visit, bool probe);

  uint64_t
  streamFrame(RunSink& sink, uint64_t bytes, FrameVisitor visit, bool probe);

  uint64_t oneShotFrame(RunSink& sink, uint64_t bytes, FrameVisitor visit);

  /// Whether a frame whose kind has `skip` frames left to store without a
  /// try is stored so; counts it down if so.
  bool skipFrame(uint32_t& skip) const;

  /// Learns from a frame tried: one that did not shrink has the next ones
  /// of its kind stored without a try.
  void learn(uint32_t& skip, bool shrank) const;

  /// Writes the run uncompressed: COMBINED_UNCOMPRESSED.
  void storeRun(
      RunSink& sink,
      uint32_t numStreams,
      const uint64_t* sizes,
      RunStreamVisitor visit);

  /// Counts a frame of `bytes`, compressed into `stored` bytes (0: stored
  /// as it is), that was tried, or not when `skipped`.
  void countFrame(uint64_t stored, uint64_t bytes, bool skipped);

  memory::MemoryPool* const pool_;
  uint64_t* const compressTimeNs_;
  const bool adaptive_;
  /// Per stream, its SEPARATE frames still to store without a try; reset
  /// when a run comes with another number of streams.
  std::vector<uint32_t> streamSkips_;
  /// The same for COMBINED frames, and for null bodies.
  uint32_t combinedSkip_{0};
  uint32_t nullSkip_{0};
  Stats stats_;
  std::vector<StreamStats> streamStats_;
  std::unique_ptr<Codec> codec_;
  /// Null when one-shot.
  std::unique_ptr<StreamCompressor> streamCompressor_;
  /// streamCompressor_'s input staging and output buffers, fixed sizes for
  /// the compressor's life.
  std::vector<uint8_t> stage_;
  std::vector<uint8_t> out_;
  /// One-shot workspaces: the gathered frame and its compressed form, or a
  /// null body's.
  BufferPtr frame_;
  BufferPtr compressed_;
  /// write()'s stream sizes.
  std::vector<uint64_t> sizes_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
