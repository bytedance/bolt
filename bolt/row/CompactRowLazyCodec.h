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

#include "bolt/vector/LazyComplexCodec.h"

namespace bytedance::bolt::row {

class CompactRowLazyCodec : public LazyComplexCodec {
 public:
  std::string_view name() const override {
    return "compact_row";
  }

  std::shared_ptr<LazyComplexVector> encode(
      const VectorPtr& input,
      memory::MemoryPool* pool) const override;

  VectorPtr decode(
      const LazyComplexVector& lazy,
      const SelectivityVector& rows,
      memory::MemoryPool* pool) const override;
};

/// Registers the CompactRow lazy codec in the global LazyComplexCodec
/// registry exactly once. Must be called before
/// `LazyComplexCodec::setActiveFormat("compact_row")`.
///
/// Static-init registration is unreliable across static-library boundaries
/// (the linker may drop the translation unit if nothing else references it),
/// so integration binaries that use the lazy codec must call this explicitly.
/// Tests wrap it automatically via `ScopedActiveLazyFormat`.
void ensureCompactRowLazyCodecRegistered();

} // namespace bytedance::bolt::row
