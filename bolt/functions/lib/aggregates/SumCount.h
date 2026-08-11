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

namespace bytedance::bolt::functions::aggregate {

// Intermediate accumulator layout for AVG: a running sum and a count of
// non-null inputs. This is the single source of truth for the AVG intermediate
// memory layout. The hash-aggregation JIT codegen derives its field offsets
// from this struct (via offsetof) instead of mirroring the layout, so any
// change here is automatically picked up by the JIT path.
//
// Keep this header dependency-free (header-only template, no .cpp / no external
// symbols) so it can be included by both the non-JIT aggregates and the JIT
// module (bolt_thrustjit) without introducing inter-library link dependencies.
template <typename TSum>
struct SumCount {
  TSum sum{0};
  int64_t count{0};
};

} // namespace bytedance::bolt::functions::aggregate
