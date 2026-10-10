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

#include <cstdint>
#include "bolt/dwio/common/IntCodecCommon.h"
#include "bolt/type/Timestamp.h"

namespace bytedance::bolt::dwrf::detail {

// Decode the seconds and tagged nanoseconds streams used by ORC and DWRF.
inline Timestamp decodeTimestamp(int64_t seconds, uint64_t encodedNanos) {
  const auto zeros = encodedNanos & 0x7;
  // The stream uses unsigned RLE, but Arrow ORC writers can encode negative
  // fractional nanos. Preserve the sign when removing the trailing-zero tag.
  auto nanos = static_cast<int64_t>(encodedNanos) >> 3;
  if (zeros != 0) {
    for (uint64_t i = 0; i <= zeros; ++i) {
      nanos *= 10;
    }
  }
  seconds += dwio::common::EPOCH_OFFSET;
  if (nanos < 0) {
    // Bolt timestamps require a nonnegative fractional part. This adjustment
    // replaces, rather than adds to, the legacy negative-seconds correction.
    --seconds;
    nanos += 1'000'000'000;
  } else if (seconds < 0 && nanos != 0) {
    // Preserve the existing ORC/DWRF convention for positive fractional nanos.
    --seconds;
  }
  return Timestamp(seconds, nanos);
}

} // namespace bytedance::bolt::dwrf::detail
