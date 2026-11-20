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
#include <cstring>
#include <limits>
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

  /// Upper bound on bytes still available, SIZE_MAX when unknown. The
  /// decoder compares declared lengths against it before allocating or
  /// reading.
  virtual size_t remainingBound() const {
    return std::numeric_limits<size_t>::max();
  }
};

class MemoryByteSource final : public ShuffleByteSource {
 public:
  explicit MemoryByteSource(std::span<const uint8_t> bytes)
      : data_(bytes.data()), size_(bytes.size()) {}

  bool read(void* out, size_t n) override {
    if (size_ - pos_ < n) {
      return false;
    }
    if (n != 0) {
      ::memcpy(out, data_ + pos_, n);
    }
    pos_ += n;
    return true;
  }

  bool atEnd() override {
    return pos_ == size_;
  }

  size_t remainingBound() const override {
    return size_ - pos_;
  }

 private:
  const uint8_t* const data_;
  const size_t size_;
  size_t pos_{0};
};

/// The codec, decompression side. The format never identifies a codec;
/// writer and reader are configured with the same one by the outer
/// protocol.
class ShuffleDecompressor {
 public:
  virtual ~ShuffleDecompressor() = default;

  /// Decompresses `in` into exactly out.size() bytes at `out`; false when
  /// the input cannot be decoded or produces a different length.
  virtual bool decompress(
      std::span<const uint8_t> in,
      std::span<uint8_t> out) = 0;
};

} // namespace bytedance::bolt::shuffle::sparksql
