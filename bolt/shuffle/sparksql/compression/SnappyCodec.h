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

#pragma once

#include "bolt/shuffle/sparksql/compression/Codec.h"

#include <cstdint>

namespace bytedance::bolt::shuffle::sparksql {

class SnappyCodec : public Codec {
 public:
  explicit SnappyCodec(const CodecOptions& options);

  int32_t defaultCompressionLevel() const override {
    return kDefaultCompressionLevel;
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

} // namespace bytedance::bolt::shuffle::sparksql
