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

#include "bolt/vector/TypeAliases.h"

#include <cstdint>

namespace bytedance::bolt::exec::bm {

struct ChunkData;

// Batch-only append range. A range maps a contiguous slice of source rows to a
// contiguous row-block slice inside one BM chunk.
struct BatchAppendRange {
  ChunkData* chunk{nullptr};
  char* rowBegin{nullptr};
  vector_size_t sourceBegin{0};
  vector_size_t rowCount{0};
};

enum class BmBatchStringStoreMode {
  kCopy,
  // Benchmark-only mode. Stored StringViews reference the input vectors, so the
  // resulting container must not outlive those vectors and must not spill.
  kReferenceInputStringForBenchmark,
};

} // namespace bytedance::bolt::exec::bm
