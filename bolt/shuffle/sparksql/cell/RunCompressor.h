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
#include <span>
#include <string>
#include <vector>

#include "bolt/buffer/Buffer.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/compression/Codec.h"
#include "bolt/shuffle/sparksql/compression/StreamCodec.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

/// Where a run is written: a spill file, the data file or a payload in
/// memory. A run's header precedes its frames, whose sizes are known only
/// once they are written, so reserve() holds header space and patch() fills
/// it in afterwards.
class RunSink {
 public:
  virtual ~RunSink() = default;

  /// Reserves `bytes` placeholder bytes in the sink and returns their starting
  /// offset for a later patch().
  virtual uint64_t reserve(size_t bytes) = 0;

  virtual void write(const void* data, size_t bytes) = 0;

  /// Overwrites `bytes` already written or reserved at `position`.
  virtual void patch(uint64_t position, const void* data, size_t bytes) = 0;
};

/// A RunSink appending to a buffer from `pool`: a payload sent whole.
class BufferRunSink final : public RunSink {
 public:
  BufferRunSink(BufferPtr& buffer, memory::MemoryPool* pool)
      : buffer_(buffer), pool_(pool) {}

  uint64_t reserve(size_t bytes) override;

  void write(const void* data, size_t bytes) override;

  void patch(uint64_t position, const void* data, size_t bytes) override;

 private:
  BufferPtr& buffer_;
  memory::MemoryPool* const pool_;
};

/// Per-kind adaptive skip policy: after a frame fails to shrink, skips the
/// next kRetryInterval - 1 frames of the same kind before probing again.
class AdaptiveSkipper {
 public:
  explicit AdaptiveSkipper(bool enabled) : enabled_(enabled) {}

  void prepareStreams(uint32_t numStreams) {
    if (streamCounters_.size() != numStreams) {
      streamCounters_.assign(numStreams, 0);
      combinedCounter_ = 0;
    }
  }

  bool shouldSkipStream(uint32_t stream) {
    return consumeSkip(streamCounters_[stream]);
  }
  void recordStream(uint32_t stream, bool shrank) {
    record(streamCounters_[stream], shrank);
  }

  bool shouldSkipCombined() {
    return consumeSkip(combinedCounter_);
  }
  void recordCombined(bool shrank) {
    record(combinedCounter_, shrank);
  }

  bool shouldSkipNullBody() {
    return consumeSkip(nullBodyCounter_);
  }
  void recordNullBody(bool shrank) {
    record(nullBodyCounter_, shrank);
  }

 private:
  /// After a frame that does not shrink, one frame in this many is tried and
  /// the others are stored without a codec call.
  static constexpr uint32_t kRetryInterval = 10;

  bool consumeSkip(uint32_t& remaining) const {
    if (!enabled_ || remaining == 0) {
      return false;
    }
    --remaining;
    return true;
  }

  void record(uint32_t& remaining, bool shrank) const {
    if (enabled_ && !shrank) {
      remaining = kRetryInterval - 1;
    }
  }

  const bool enabled_;
  std::vector<uint32_t> streamCounters_;
  uint32_t combinedCounter_{0};
  uint32_t nullBodyCounter_{0};
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
/// A run below kMinSeparateRunBytes is one kCombined frame over all its
/// streams; a larger one is kSeparate, every stream its own frame, so each
/// stream's data alone sets its ratio. A frame below kMinCompressBytes
/// is stored, and so is a frame that does not shrink: it is then cheaper to
/// read as it is. That is decided only while the whole frame so far is
/// still in memory - after its first blocks of input, or at its end when
/// it is no larger - so nothing written is ever taken back: once a frame
/// reaches the sink it is kept compressed. A kSeparate frame is one stream,
/// whose first blocks speak for the rest; a kCombined frame, spanning
/// streams, is judged on its whole size only.
///
/// Adaptive, a frame that does not shrink has the next frames of the same
/// kind stored without a codec call, the tenth tried again: frames of a
/// stream for kSeparate runs, and all kCombined frames together, which span
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
  /// If `streamCompressor` is supplied, it is used as the stream form instead
  /// of creating one from `options`.
  RunCompressor(
      const PartitionWriterOptions& options,
      const CellShuffleOptions& cellOptions,
      uint64_t* compressTimeNs,
      std::unique_ptr<StreamCompressor> streamCompressor = nullptr);

  ~RunCompressor();

  /// The codec, to read compressed runs back; null when runs are not
  /// compressed.
  Codec* codec() const {
    return codec_.get();
  }

  /// Writes one run of `numStreams` streams, `size(s)` bytes each, that
  /// `visit` hands over - twice for a frame stored after all. The sizes
  /// come first: they choose the layout and fill the header. A run without
  /// bytes writes nothing and returns 0; otherwise returns the total
  /// uncompressed payload bytes across all streams.
  uint64_t write(
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

  /// Releases the one-shot compression buffers back to the spill pool.
  void releaseBuffers();

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
    /// Tried, and stored because they did not shrink; `earlyStoredFrames` of
    /// them given up after their first blocks.
    uint64_t unshrunkFrames{0};
    uint64_t unshrunkBytes{0};
    uint64_t earlyStoredFrames{0};
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

  const Stats& stats() const {
    return stats_;
  }

 private:
  using FrameVisitor = folly::FunctionRef<void(RunPieceFn fn)>;

  /// Writes the `bytes` `visit` hands over as one compressed frame, unless
  /// it does not shrink - judged early from its first blocks when `probeEarly`,
  /// else on its whole size. Returns the compressed frame's size; 0 when it
  /// was not compressed, and nothing was written.
  uint64_t writeFrame(
      RunSink& sink,
      uint64_t bytes,
      FrameVisitor visit,
      bool probeEarly);

  uint64_t streamFrame(
      RunSink& sink,
      uint64_t totalInputBytes,
      FrameVisitor visit,
      bool probeEarly);

  uint64_t oneShotFrame(RunSink& sink, uint64_t bytes, FrameVisitor visit);

  /// Writes the run of sizes.size() streams uncompressed:
  /// kCombinedUncompressed.
  void storeUncompressed(
      RunSink& sink,
      std::span<const uint64_t> sizes,
      uint64_t total,
      RunStreamVisitor visit);

  /// Counts a frame of `bytes`, compressed into `stored` bytes (0: stored
  /// as it is), that was tried, or not when `skipped`.
  void countFrame(uint64_t stored, uint64_t bytes, bool skipped);

  memory::MemoryPool* const spillPool_;
  uint64_t* const compressTimeNs_;
  AdaptiveSkipper skipper_;
  Stats stats_;
  std::unique_ptr<Codec> codec_;
  /// Null when one-shot.
  std::unique_ptr<StreamCompressor> streamCompressor_;
  /// streamCompressor_'s input staging and output buffers, fixed sizes for
  /// the compressor's life.
  std::vector<uint8_t> streamInputStage_;
  std::vector<uint8_t> streamOutputBuffer_;
  /// One-shot workspaces: the gathered frame and its compressed form, or a
  /// null body's.
  BufferPtr frame_;
  BufferPtr compressed_;
  /// Reusable stream-size scratch for write().
  std::vector<uint64_t> sizes_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
