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

#include "bolt/exec/bm/BmRowContainerPublicTypes.h"

#include <folly/Portability.h>

namespace bytedance::bolt::exec::bm {

class BmRowContainer;
struct BlockRef;
struct ChunkData;
struct SegmentData;

// Location token returned by BmRowContainer::appendRow(). It is only meant for
// immediately storing columns of that row; do not keep it after flush.
class RowWriteContext {
 public:
  RowWriteContext() = default;

  FOLLY_ALWAYS_INLINE char* row() const {
    return row_;
  }

  FOLLY_ALWAYS_INLINE SegmentData* segment() const {
    return segment_;
  }

  FOLLY_ALWAYS_INLINE ChunkData* chunk() const {
    return chunk_;
  }

 private:
  friend class BmRowContainer;

  RowWriteContext(SegmentData* segment, ChunkData* chunk, char* row)
      : segment_(segment), chunk_(chunk), row_(row) {}

  SegmentData* segment_{nullptr};
  ChunkData* chunk_{nullptr};
  BlockRef* currentHeap_{nullptr};
  BlockId recordedHeapBlock_{kNoBlock};
  char* row_{nullptr};
};

} // namespace bytedance::bolt::exec::bm
