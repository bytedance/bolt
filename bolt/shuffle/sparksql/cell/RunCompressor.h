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

/// The Cell writer's run compression, one policy for every run it writes:
/// spilled, merged, or sent in an RSS payload.
///
/// A run below kMinSeparateRunBytes is one COMBINED frame over all its
/// streams; a larger one is SEPARATE, every stream its own frame, so each
/// stream's data alone sets its ratio. A frame below kMinCompressRunBytes
/// is stored, and so is a frame that does not shrink: it is then cheaper to
/// read as it is. That is decided only while the whole frame so far is
/// still in memory - after its first blocks of input, or at its end when
/// it is no larger - so nothing written is ever taken back: once a frame
/// reaches the sink it is kept compressed. A SEPARATE frame is one stream,
/// whose first blocks speak for the rest; a COMBINED frame, spanning
/// streams, is judged on its whole size only.
///
/// Streaming compresses a frame straight from the visitor through fixed
/// buffers: no workspace sized by the data, ever. One-shot gathers each
/// frame into a workspace from the pool and compresses it at once; a frame
/// whose workspace cannot be funded is stored instead. Codecs with no
/// stream form are always one-shot. Both write the same layouts.
class RunCompressor {
 public:
  /// Compresses with `options`' codec; stores everything when it is
  /// UNCOMPRESSED. Codec time is added to `*compressTimeNs`; `pool` funds
  /// the one-shot workspaces and must outlive this object.
  RunCompressor(
      const PartitionWriterOptions& options,
      bool streaming,
      memory::MemoryPool* pool,
      uint64_t* compressTimeNs);

  ~RunCompressor();

  /// Whether runs are compressed at all.
  bool compresses() const {
    return codec_ != nullptr;
  }

  /// The codec, to read compressed runs back; null when compresses() is
  /// false.
  Codec* codec() const {
    return codec_.get();
  }

  /// Writes one run of `numStreams` streams, `sizes[s]` bytes each, that
  /// `visit` hands over - twice for a frame stored after all. Returns false
  /// when a frame was stored for want of a one-shot workspace: a valid run
  /// still, which a later pass may compress.
  bool write(
      RunSink& sink,
      uint32_t numStreams,
      const uint64_t* sizes,
      RunStreamVisitor visit);

  /// Writes the run uncompressed: COMBINED_UNCOMPRESSED.
  void writeStored(
      RunSink& sink,
      uint32_t numStreams,
      const uint64_t* sizes,
      RunStreamVisitor visit);

  /// Gives the one-shot workspaces back to the pool.
  void releaseWorkspace();

 private:
  using FrameVisitor = folly::FunctionRef<void(RunPieceFn fn)>;

  /// How a frame went.
  struct Frame {
    /// The compressed frame's size; 0 when it was not compressed, and
    /// nothing was written.
    uint64_t stored{0};
    /// False when not compressed for want of a one-shot workspace, rather
    /// than because it does not shrink.
    bool final{true};
  };

  /// Writes the `bytes` `visit` hands over as one compressed frame, unless
  /// it does not shrink - judged early from its first blocks when `probe`,
  /// else on its whole size - or its one-shot workspace cannot be funded.
  Frame
  writeFrame(RunSink& sink, uint64_t bytes, FrameVisitor visit, bool probe);

  uint64_t
  streamFrame(RunSink& sink, uint64_t bytes, FrameVisitor visit, bool probe);

  Frame oneShotFrame(RunSink& sink, uint64_t bytes, FrameVisitor visit);

  memory::MemoryPool* const pool_;
  uint64_t* const compressTimeNs_;
  std::unique_ptr<Codec> codec_;
  /// Null when one-shot.
  std::unique_ptr<StreamCompressor> streamCompressor_;
  /// streamCompressor_'s input staging and output buffers, fixed sizes for
  /// the compressor's life.
  std::vector<uint8_t> stage_;
  std::vector<uint8_t> out_;
  /// One-shot workspaces: the gathered frame and its compressed form.
  BufferPtr frame_;
  BufferPtr compressed_;
};

} // namespace bytedance::bolt::shuffle::sparksql::cell
