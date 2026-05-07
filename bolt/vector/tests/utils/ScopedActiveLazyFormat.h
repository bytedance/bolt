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

#include "bolt/row/CompactRowLazyCodec.h"
#include "bolt/vector/LazyComplexCodec.h"

namespace bytedance::bolt::test {

/// RAII helper that activates a named lazy-complex codec format for the
/// duration of a test and restores the previous setting on destruction.
/// Only for use in tests.
class ScopedActiveLazyFormat {
 public:
  explicit ScopedActiveLazyFormat(std::string_view name)
      : previous_(
            LazyComplexCodec::activeCodec()
                ? std::string(LazyComplexCodec::activeCodec()->name())
                : std::string()) {
    // Ensure built-in codecs are registered before we try to activate one.
    // Relying on static-init across static-library boundaries is fragile;
    // this explicit call is the supported entry point.
    if (name == "compact_row") {
      row::ensureCompactRowLazyCodecRegistered();
    }
    LazyComplexCodec::setActiveFormat(name);
  }

  ~ScopedActiveLazyFormat() {
    LazyComplexCodec::setActiveFormat(previous_);
  }

  ScopedActiveLazyFormat(const ScopedActiveLazyFormat&) = delete;
  ScopedActiveLazyFormat& operator=(const ScopedActiveLazyFormat&) = delete;

 private:
  std::string previous_;
};

} // namespace bytedance::bolt::test
