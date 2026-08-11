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
 * 2026-08-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <cstdint>

#include "bolt/type/HugeInt.h"

// Single source of truth for the in-memory layout of the decimal sum / decimal
// avg accumulators. The non-JIT accumulators (DecimalSum,
// LongDecimalWithOverflowState) derive from these POD layout bases, and the
// hash-aggregation JIT codegen aliases the same bases to compute field offsets
// (via offsetof) instead of mirroring the layout. A change here is therefore
// picked up by both paths automatically.
//
// Keep this header dependency-free (header-only PODs, no .cpp / no external
// symbols) so it can be included by both the non-JIT aggregates and the JIT
// module (bolt_thrustjit) without introducing inter-library link dependencies.
//
// IMPORTANT: classes deriving from these layouts must add *no* non-static data
// members (methods only), otherwise they stop being standard-layout and the
// offsets the JIT relies on become undefined. Each derived type guards this
// with static_assert(std::is_standard_layout_v<...>).
namespace bytedance::bolt::functions::aggregate {

// Layout of the decimal sum accumulator: ROW(sum, isEmpty) with an overflow
// counter. Field order is part of the contract shared with the JIT path.
struct DecimalSumAccumulatorLayout {
  int128_t sum{0};
  int64_t overflow{0};
  bool isEmpty{true};
};

// Layout of the long-decimal-with-overflow accumulator (used by decimal avg):
// running sum, count of rows, and net overflow counter. NOTE: this is the
// in-memory layout {sum, count, overflow}, which differs from the serialized
// byte order {count, overflow, sum}; the JIT path reads memory, not serialized
// form, so it aligns with this layout.
struct LongDecimalWithOverflowLayout {
  int128_t sum{0};
  int64_t count{0};
  int64_t overflow{0};
};

} // namespace bytedance::bolt::functions::aggregate
