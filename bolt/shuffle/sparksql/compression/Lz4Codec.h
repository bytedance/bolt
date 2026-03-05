/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/shuffle/sparksql/compression/Codec.h"

#include <lz4.h>

#include <lz4frame.h>
#include <cstdint>

namespace bytedance::bolt::shuffle::sparksql {

constexpr int kLz4DefaultCompressionLevel = 1;

class Lz4Codec : public Codec {
 public:
  explicit Lz4Codec(const CodecOptions& options);

  int32_t defaultCompressionLevel() const override {
    return kLz4DefaultCompressionLevel;
  }

  int64_t compress(
      const uint8_t* input,
      int64_t inputLength,
      uint8_t* output,
      int64_t outputLength) override;

  int64_t decompress(
      const uint8_t* input,
      int64_t inputLength,
      uint8_t* output,
      int64_t outputLength) override;

  int64_t maxCompressedLen(int64_t inputLength) const override;
};

class Lz4FrameCodec : public Codec {
 public:
  explicit Lz4FrameCodec(const CodecOptions& options);

  int32_t defaultCompressionLevel() const override {
    return kLz4DefaultCompressionLevel;
  }

  int64_t compress(
      const uint8_t* input,
      int64_t inputLength,
      uint8_t* output,
      int64_t outputLength) override;

  int64_t decompress(
      const uint8_t* input,
      int64_t inputLength,
      uint8_t* output,
      int64_t outputLength) override;

  int64_t maxCompressedLen(int64_t inputLength) const override;

 private:
  const LZ4F_preferences_t prefs_;
};

} // namespace bytedance::bolt::shuffle::sparksql
