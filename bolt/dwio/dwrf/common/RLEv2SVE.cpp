/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/dwio/dwrf/common/RLEv2SVE.h"

#include <arm_sve.h>
#include <algorithm>

namespace bytedance::bolt::dwrf::detail {
namespace {

svbool_t validLanes(const uint64_t* nulls, uint64_t pos, uint64_t end) {
  const auto tail = svwhilelt_b64(pos, end);
  if (!nulls) {
    return tail;
  }
  const auto count = std::min<uint64_t>(svcntd(), end - pos);
  const auto bit = pos & 63;
  uint64_t valid = nulls[pos >> 6] >> bit;
  // Do not read beyond the bitmap at a partial final vector.
  if (bit + count > 64) {
    valid |= nulls[(pos >> 6) + 1] << (64 - bit);
  }
  const auto lanes = svindex_u64(0, 1);
  const auto bits = svlsr_u64_x(tail, svdup_n_u64(valid), lanes);
  return svcmpne_n_u64(tail, svand_n_u64_x(tail, bits, 1), 0);
}

// Unsigned arithmetic preserves the decoder's two's-complement bit patterns
// even when a prefix sum crosses bit 63. Out-of-range table indices yield zero.
svuint64_t inclusiveSum(svuint64_t values) {
  const auto all = svptrue_b64();
  const auto lanes = svindex_u64(0, 1);
  for (uint64_t shift = 1; shift < svcntd(); shift *= 2) {
    const auto previous = svtbl_u64(values, svsub_n_u64_x(all, lanes, shift));
    values = svadd_u64_x(all, values, previous);
  }
  return values;
}

} // namespace

uint64_t fillRepeatedSve(
    int64_t* data,
    uint64_t offset,
    uint64_t end,
    const uint64_t* nulls,
    int64_t value) {
  uint64_t count = 0;
  const auto repeated = svdup_n_s64(value);
  for (auto pos = offset; pos < end; pos += svcntd()) {
    const auto valid = validLanes(nulls, pos, end);
    svst1_s64(valid, data + pos, repeated);
    count += svcntp_b64(svptrue_b64(), valid);
  }
  return count;
}

void zigzagDecodeSve(
    int64_t* data,
    uint64_t offset,
    uint64_t end,
    const uint64_t* nulls) {
  for (auto pos = offset; pos < end; pos += svcntd()) {
    const auto valid = validLanes(nulls, pos, end);
    auto* values = reinterpret_cast<uint64_t*>(data + pos);
    const auto encoded = svld1_u64(valid, values);
    const auto high = svlsr_n_u64_x(valid, encoded, 1);
    const auto low = svand_n_u64_x(valid, encoded, 1);
    const auto sign = svsub_u64_x(valid, svdup_n_u64(0), low);
    svst1_u64(valid, values, sveor_u64_x(valid, high, sign));
  }
}

uint64_t fixedDeltaSve(
    int64_t* data,
    uint64_t offset,
    uint64_t end,
    const uint64_t* nulls,
    int64_t delta,
    int64_t& previous) {
  uint64_t count = 0;
  auto carry = static_cast<uint64_t>(previous);
  const auto step = static_cast<uint64_t>(delta);
  const auto all = svptrue_b64();
  for (auto pos = offset; pos < end; pos += svcntd()) {
    const auto valid = validLanes(nulls, pos, end);
    const auto rank = nulls
        ? inclusiveSum(svsel_u64(valid, svdup_n_u64(1), svdup_n_u64(0)))
        : svindex_u64(1, 1);
    const auto values =
        svadd_n_u64_x(valid, svmul_n_u64_x(valid, rank, step), carry);
    svst1_u64(valid, reinterpret_cast<uint64_t*>(data + pos), values);
    const auto written = svcntp_b64(all, valid);
    carry += written * step;
    count += written;
  }
  previous = static_cast<int64_t>(carry);
  return count;
}

void variableDeltaSve(
    int64_t* data,
    uint64_t offset,
    uint64_t end,
    const uint64_t* nulls,
    bool negative,
    int64_t& previous) {
  auto carry = static_cast<uint64_t>(previous);
  const auto all = svptrue_b64();
  for (auto pos = offset; pos < end; pos += svcntd()) {
    const auto valid = validLanes(nulls, pos, end);
    auto* values = reinterpret_cast<uint64_t*>(data + pos);
    // Predicated loads zero null and tail lanes before the prefix sum.
    const auto deltas = svld1_u64(valid, values);
    const auto sums = inclusiveSum(deltas);
    const auto base = svdup_n_u64(carry);
    const auto decoded = negative ? svsub_u64_x(valid, base, sums)
                                  : svadd_u64_x(valid, base, sums);
    svst1_u64(valid, values, decoded);
    const auto total = svaddv_u64(all, deltas);
    carry = negative ? carry - total : carry + total;
  }
  previous = static_cast<int64_t>(carry);
}

} // namespace bytedance::bolt::dwrf::detail
