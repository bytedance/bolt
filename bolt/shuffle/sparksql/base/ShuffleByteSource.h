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

#include <cstddef>
#include <cstdint>
#include <span>

namespace bytedance::bolt::shuffle::sparksql {

/// Pull-based byte input. Reads are exact: short input is a format error the
/// decoder reports, so read() returns false rather than a partial count.
class ShuffleByteSource {
 public:
  virtual ~ShuffleByteSource() = default;

  /// Reads exactly `n` bytes into out. False when the source ends first.
  virtual bool read(void* out, size_t n) = 0;

  /// True when no byte is left. Lets a stream reader distinguish "clean end
  /// between blocks" from truncation.
  virtual bool atEnd() = 0;
};

/// The codec, decompression side. The format never identifies a codec;
/// writer and reader are configured with the same one by the outer
/// protocol.
class ShuffleDecompressor {
 public:
  virtual ~ShuffleDecompressor() = default;

  /// Decompresses `in` into exactly out.size() bytes at `out`. Returns false
  /// when decompression produces a different length (or a non-throwing
  /// implementation fails to decode); codec-backed implementations may also
  /// throw BoltRuntimeError on corrupt frames or checksum mismatches.
  virtual bool decompress(
      std::span<const uint8_t> in,
      std::span<uint8_t> out) = 0;
};

} // namespace bytedance::bolt::shuffle::sparksql
