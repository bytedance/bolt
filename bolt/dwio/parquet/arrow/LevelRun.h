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
 * 2026-11-19.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

// Adapted from Apache Arrow.

#pragma once

#include <cstdint>

#include "arrow/buffer_builder.h"

namespace bytedance::bolt::parquet::arrow {

// Coordinates refer to the dense level sequence for one leaf write. Runs
// must be sorted, non-overlapping and agree with the retained dense array.
struct LevelRun {
  int64_t offset;
  int64_t count;
  int16_t value;
};

// Borrowed from ArrowWriteContext; valid only during the leaf callback.
struct LevelRunView {
  const LevelRun* data = nullptr;
  int64_t size = 0;
};

struct LevelRunHints {
  LevelRunView definition;
  LevelRunView repetition;
};

// Retained by one ArrowWriteContext; the buffers use its writer memory pool.
struct LevelRunScratch {
  explicit LevelRunScratch(::arrow::MemoryPool* pool)
      : definition(pool), repetition(pool) {}

  ::arrow::TypedBufferBuilder<LevelRun> definition;
  ::arrow::TypedBufferBuilder<LevelRun> repetition;
};

inline constexpr int64_t kMinLevelRunLength = 32;

} // namespace bytedance::bolt::parquet::arrow
