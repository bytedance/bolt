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

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/bm/BmSegmentCollection.h"

#include <folly/Portability.h>

#include <vector>

namespace bytedance::bolt::exec::bm {

// Copies an already-resident row into another segment. Used by reordered
// segment materialization so merge cursors can scan rows in physical order.
class BmRowCopier {
 public:
  BmRowCopier(
      const std::vector<TypePtr>* types,
      const BmRowLayout* layout,
      BmSegmentCollection* segments);

  char* copyRowToSegment(SegmentData& segment, const char* source);

 private:
  FOLLY_ALWAYS_INLINE const std::vector<TypePtr>& types() const {
    BOLT_DCHECK_NOT_NULL(types_);
    return *types_;
  }

  FOLLY_ALWAYS_INLINE const BmRowLayout& layout() const {
    BOLT_DCHECK_NOT_NULL(layout_);
    return *layout_;
  }

  FOLLY_ALWAYS_INLINE BmSegmentCollection& segments() const {
    BOLT_DCHECK_NOT_NULL(segments_);
    return *segments_;
  }

  const std::vector<TypePtr>* types_{nullptr};
  const BmRowLayout* layout_{nullptr};
  BmSegmentCollection* segments_{nullptr};
};

} // namespace bytedance::bolt::exec::bm
